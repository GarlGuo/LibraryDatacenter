from pymongo import MongoClient
from pymongo import InsertOne, DeleteOne, ReplaceOne  # bulk_writes
import os
from os import getcwd, sep
from numpy import ndarray
from pymongo.errors import OperationFailure
from pymongo.errors import BulkWriteError
from pprint import pprint
import json
from random import randint
from pymongo.errors import ConnectionFailure, OperationFailure
import numpy as np
from redis_util import *
import hdfs_file_util
from server_status import checkReplStatus, checkWorkLoad
import time


def classify_user(user_str):
    return 0 if 'Beijing' in user_str else 1


def ALL_PASS(): return True


def isTechnology(article_str): return "technology" in article_str


def isScience(article_str): return "science" in article_str


class MongoDBInserter:
    def __init__(self):
        pass

    def finishOneCollection(self, col, arr, ptr):
        if ptr != 0:
            col.bulk_write(arr[:ptr], ordered=True)

    def testOneBulkWrite(self, col, arr, ptr, bulkSize):
        if ptr == bulkSize:
            ptr = 0
            col.bulk_write(arr, ordered=True)
        return ptr

    def insert_one_collection_raw_data(self, col, iter, bulk_size):
        arr = list(range(bulk_size))
        ptr = 0
        for r in iter.find():
            del r['_id']
            ptr = self.testOneBulkWrite(col, arr, ptr, bulk_size)
            arr[ptr] = InsertOne(r)
            ptr += 1
        self.finishOneCollection(col, arr, ptr)

    def process_one_file_one_collection(self, file, col, bulk_size, pred):
        arr = list(range(bulk_size))
        ptr = 0
        with open(file, "r") as f:
            for line in f.readlines():
                if pred(line) == False:
                    continue
                ptr = self.testOneBulkWrite(col, arr, ptr, bulk_size)
                arr[ptr] = InsertOne(json.loads(line))
                ptr += 1
        self.finishOneCollection(col, arr, ptr)

    def process_one_file_multiple_collection(self, file, cols, size, pred):
        arrs = [list(range(size)) for _ in cols]
        length = len(cols)
        ptrs, count = np.zeros(length, dtype=np.int64), np.zeros(
            length, dtype=np.int64)
        with open(file, "r") as f:
            for line in f.readlines():
                pos = pred(line)
                count[pos] += 1
                ptrs[pos] = self.testOneBulkWrite(
                    cols[pos], arrs[pos], ptrs[pos], size)
                arrs[pos][ptrs[pos]] = InsertOne(json.loads(line))
                ptrs[pos] = ptrs[pos] + 1
        for i, one_col in enumerate(cols):
            self.finishOneCollection(one_col, arrs[i], ptrs[i])
            print(f"col {one_col.full_name} insertion count: {count[i]}")

    def process_one_file_multiple_collection_rand(self, fileAddr, cols, size, pred):
        arrs = [list(range(size)) for _ in enumerate(cols)]
        length = len(cols)
        ptrs, count = np.zeros(length, dtype=np.int64), np.zeros(
            length, dtype=np.int64)
        with open(fileAddr, "r") as f:
            for line in f.readlines():
                if pred(line) == False:
                    continue
                randPos = randint(0, length - 1)
                count[randPos] += 1
                ptrs[randPos] = self.testOneBulkWrite(
                    cols[randPos], arrs[randPos], ptrs[randPos], size)
                arrs[randPos][ptrs[randPos]] = InsertOne(json.loads(line))
                ptrs[randPos] = ptrs[randPos] + 1
        for i, one_col in enumerate(cols):
            self.finishOneCollection(one_col, arrs[i], ptrs[i])
            print(f"col {one_col.full_name} insertion count: {count[i]}")


mongoDBInserter = MongoDBInserter()


class MongoDBUpdater:
    def __init__(self):
        pass

    @staticmethod
    def update(col, filter_repl_list, sizeForNewBulk):
        length = len(filter_repl_list)
        count = 0
        countUpTo = sizeForNewBulk - length
        arr = np.zeros(sizeForNewBulk, InsertOne)
        while countUpTo > count:
            arr = [ReplaceOne(filt, repl)
                   for filt, repl in filter_repl_list[count: count + sizeForNewBulk]]
            count += sizeForNewBulk
            col.bulk_write(arr.tolist())
        arr = [json.loads(jsStr)
               for jsStr in filter_repl_list[count:]]
        col.bulk_write()


mongoDBUpdater = MongoDBUpdater()


class MongoDBManager:

    @staticmethod
    def classifyNodes(hostList):
        res = dict()
        for oneHost in hostList:
            try:
                client = MongoClient(host=oneHost)
                client.server_info()  # checkConnection
                repl_info = client.admin.command({'replSetGetStatus': 1})
                for node in repl_info['members']:
                    if node['health'] == 1:
                        res[node['name']] = node['stateStr']
            except:
                continue
        if len(res) == 0:
            raise "No Active server available in list" + str(hostList)
        return res

    @staticmethod
    def getSecondaryList(hostList, prevRes=None):
        if prevRes != None:
            return [node for node in prevRes if prevRes[node] == 'SECONDARY']
        res = MongoDBManager.classifyNodes(hostList)
        return [node for node in res if res[node] == 'SECONDARY']

    @staticmethod
    def getPrimaryNode(hostList, prevRes=None):
        if prevRes != None:
            return [node for node in prevRes if prevRes[node] == 'PRIMARY']
        res = MongoDBManager.classifyNodes(hostList)
        availableNodes = MongoDBManager.classifyNodes(hostList)
        primaryNodeList = [
            node for node in availableNodes if availableNodes[node] == 'PRIMARY']
        if len(primaryNodeList) == 0:
            raise "no primary node available in list: " + hostList
        return primaryNodeList[0]

    @staticmethod
    def fillNPList(iter, mapper):
        res = np.zeros(iter.count(), dtype=dict)
        for i, x in enumerate(iter):
            res[i] = mapper(x)
        return res

    def initiateCollection(self, db, colName, idx_dict=None):
        if colName in db.list_collection_names():
            return db[colName]
        else:
            col = db[colName]
            if idx_dict != None:
                col.create_index(idx_dict)
            return col

    def initiate_user(self):
        if self.user_one_col.find().count() == 0 or \
                self.user_two_col.find().count() == 0:
            mongoDBInserter.process_one_file_multiple_collection(
                self.USER_DAT_ADDR,
                [self.user_one_col, self.user_two_col],
                self.USER_BULK_LIMIT,
                classify_user
            )
            lst_one = MongoDBManager.fillNPList(self.user_one_col.find(
                {}, {'uid'}), lambda r: r['uid'])
            storeUIDList(0, lst_one)
            lst_two = MongoDBManager.fillNPList(self.user_two_col.find(
                {}, {'uid'}), lambda r: r['uid'])
            storeUIDList(1, lst_two)

    def initiate_art(self):
        if self.art_one_col.find().count() == 0 or \
                self.art_two_col.find().count() == 0:
            mongoDBInserter.process_one_file_multiple_collection_rand(
                self.ARTICLE_DAT_ADDR,
                [self.art_one_col, self.art_two_col],
                self.USER_BULK_LIMIT,
                isScience
            )
            mongoDBInserter.process_one_file_one_collection(
                self.ARTICLE_DAT_ADDR,
                self.art_two_col,
                self.ARTICLE_BULK_LIMIT,
                isTechnology
            )

    def classifyUserSite(self):
        lookup = {}
        for u in self.user_one_col.find({}, {'uid'}):
            lookup[u['uid']] = 0
        for u in self.user_two_col.find({}, {'uid'}):
            lookup[u['uid']] = 1
        return lookup

    def process_read(self, inserter):
        cols = [self.read_one_col, self.read_two_col]
        lookup = self.classifyUserSite()
        arrs = [list(range(self.READ_BULK_LIMIT)),
                list(range(self.READ_BULK_LIMIT))]
        ptrs, count = [0, 0], [0, 0]
        with open(self.READ_DAT_ADDR, "r") as f:
            for line in f.readlines():
                obj = json.loads(line)
                pos = lookup[obj['uid']]
                count[pos] = count[pos] + 1
                ptrs[pos] = inserter.testOneBulkWrite(
                    cols[pos], arrs[pos], ptrs[pos], self.READ_BULK_LIMIT
                )
                arrs[pos][ptrs[pos]] = InsertOne(obj)
                ptrs[pos] = ptrs[pos] + 1
        for i, one_col in enumerate(cols):
            inserter.finishOneCollection(one_col, arrs[i], ptrs[i])
            print(f"col {one_col.full_name} insertion count: {count[i]}")

    def initiate_read(self):
        if self.read_one_col.find().count() == 0 or \
                self.read_two_col.find().count() == 0:
            self.read_one_col.drop()
            self.read_two_col.drop()
            self.process_read(mongoDBInserter)

    def init_site(self):
        siteOneNodes = MongoDBManager.classifyNodes(self.siteOneHostList)
        siteTwoNodes = MongoDBManager.classifyNodes(self.siteTwoHostList)
        self.siteOnePrimary = MongoDBManager.getPrimaryNode(
            self.siteOneHostList,
            prevRes=siteOneNodes
        )
        self.siteTwoPrimary = MongoDBManager.getPrimaryNode(
            self.siteTwoHostList,
            prevRes=siteTwoNodes
        )
        print(
            f"Primary sites are {self.siteOnePrimary}, {self.siteTwoPrimary}")
        self.siteOneClient = MongoClient(host=self.siteOnePrimary)
        self.siteTwoClient = MongoClient(host=self.siteTwoPrimary)
        self.siteOneSecondaryList = MongoDBManager.getSecondaryList(
            self.siteOneHostList,
            prevRes=siteOneNodes
        )
        self.siteTwoSecondaryList = MongoDBManager.getSecondaryList(
            self.siteTwoHostList,
            prevRes=siteTwoNodes
        )

    def refreshSite(self):
        self.db1 = self.siteOneClient["proj"]
        self.db2 = self.siteTwoClient["proj"]

        self.user_one_col = self.db1['User']
        self.user_two_col = self.db2['User-2']

        self.read_one_col = self.db1['Read']
        self.read_two_col = self.db2['Read-2']
        self.read_total_col = self.db1['Read-Total']

        self.art_one_col = self.db1['Article']
        self.art_two_col = self.db2['Article-2']

        self.beread_one_col = self.db1['Be-Read']
        self.beread_two_col = self.db2['Be-Read-2']
        self.pop_one_col = self.db1["Popular-Rank"]
        self.pop_two_col = self.db2["Popular-Rank-2"]
        self.pop_total_col = self.db1["Popular-Rank-Total"]
        self.beread_total_col = self.db1['Be-Read-Total']

    def __init__(self, initialClear=False):
        hdfs_file_util.clearFileCache()
        clearRedisCache()
        self.CURRENT_DIR = getcwd()
        self.USER_DAT_ADDR = self.CURRENT_DIR + sep + "db" + sep + "user.dat"
        self.READ_DAT_ADDR = self.CURRENT_DIR + sep + "db" + sep + "read.dat"
        self.ARTICLE_DAT_ADDR = self.CURRENT_DIR + sep + "db" + sep + "article.dat"
        if os.path.exists('hdfs_file_cache') == False:
            os.mkdir('hdfs_file_cache')
        self.HDFS_FILE_CACHE = self.CURRENT_DIR + sep + 'hdfs_file_cache'

        self.ARTICLE_BULK_LIMIT = 50000
        self.USER_BULK_LIMIT = 50000
        self.READ_BULK_LIMIT = 60000

        self.redis = redis_server
        self.siteOneHostList = ("mongodb://127.0.0.1:5000/",
                                "mongodb://127.0.0.1:5001/",
                                "mongodb://127.0.0.1:5002/",
                                "mongodb://127.0.0.1:5003/")
        self.siteTwoHostList = ("mongodb://127.0.0.1:7000/",
                                "mongodb://127.0.0.1:7001/",
                                "mongodb://127.0.0.1:7002/")

        self.init_site()
        self.refreshSite()

        if initialClear:
            self.user_one_col.drop()
            self.user_two_col.drop()
            self.read_one_col.drop()
            self.read_two_col.drop()
            self.art_one_col.drop()
            self.art_two_col.drop()

        self.initiate_user()
        self.initiate_art()
        self.initiate_read()

        u_lst_one = MongoDBManager.fillNPList(self.user_one_col.find(
            {}, {'uid'}), lambda r: r['uid'])
        storeUIDList(0, u_lst_one)
        u_lst_two = MongoDBManager.fillNPList(self.user_two_col.find(
            {}, {'uid'}), lambda r: r['uid'])
        storeUIDList(1, u_lst_two)

        a_lst_one = MongoDBManager.fillNPList(self.art_one_col.find(
            {}, {'aid'}), lambda r: r['aid'])
        storeAIDList(0, a_lst_one)
        a_lst_two = MongoDBManager.fillNPList(self.art_two_col.find(
            {}, {'aid'}), lambda r: r['aid'])
        storeAIDList(1, a_lst_two)
        print("User/Article/Read table initialization finished!")

    def check_DBMS_status(self, ignore=False):
        if ignore == False:
            checkReplStatus(self.siteOneHostList)
        checkWorkLoad(self.siteOneClient, 'proj')
        if ignore == False:
            checkReplStatus(self.siteTwoHostList)
        checkWorkLoad(self.siteTwoClient, 'proj')

    def _get_total(self, col1, col2):
        c1, c2 = col1.count(), col2.count()
        res = np.zeros(c1 + c2, dtype=dict)
        res[:c1] = list(col1.find())
        res[c1:] = list(col2.find())
        return res

    def _get_total_with_process(self, col1, col2, mapper):
        idx_1, idx_2 = 0,   0
        col1_arr = np.zeros(col1.count(), dtype=dict)
        for i, x in enumerate(mapper(col1)):
            col1_arr[i] = x
            idx_1 += 1
        col2_arr = np.zeros(col2.count(), dtype=dict)
        for i, x in enumerate(mapper(col2)):
            col2_arr[i] = x
            idx_2 += 1
        res = np.append(col1_arr[:idx_1], col2_arr[:idx_2])
        return res

    @property
    def user_total(self):
        return self._get_total(self.user_one_col, self.user_two_col)

    @property
    def art_total(self):
        return self._get_total(self.art_one_col, self.art_two_col)

    @property
    def read_total(self):
        return self._get_total(self.read_one_col, self.read_two_col)

    @property
    def pop_rank_total(self):
        return self.pop_total_col

    @property
    def be_read_total(self):
        return self._get_total(self.beread_one_col, self.beread_two_col)

    def getOnePopularRankFile(self, pop_col, art_col, name):
        print(f"Popular rank for {name}")
        for r in pop_col.find({}, {'temporalGranularity',
                                   'articleAidList'}):
            print(
                f"  Granularity: {r['temporalGranularity']} Article List: {r['articleAidList']}")
            for one_art in r['articleAidList']:
                article = art_col.find_one({'aid': one_art})
                hdfs_file_util.getOneArticleByteStream(
                    article, name, r['temporalGranularity'])

    def getTotalPopularRankArticle(self):
        print(f"Popular rank for total popular-rank")
        for r in self.pop_total_col.find({}, {'temporalGranularity',
                                              'articleAidList'}):
            print(
                f"  Granularity: {r['temporalGranularity']} Article List: {r['articleAidList']}")
            for one_art in r['articleAidList']:
                if getAIDPos(one_art, self) == 0:
                    article = self.art_one_col.find_one({'aid': one_art})
                    hdfs_file_util.getOneArticleByteStream(
                        article, "total", r['temporalGranularity'])
                else:
                    article = self.art_two_col.find_one(
                        {'aid': one_art})
                    hdfs_file_util.getOneArticleByteStream(
                        article, "total", r['temporalGranularity'])

    def uploadOnePopularRankFile(self, pop_col, art_col, name):
        for one_row in pop_col.find({}, {'articleAidList'}):
            for one_art in one_row['articleAidList']:
                article = art_col.find_one({'aid': one_art})
                hdfs_file_util.uploadDirectoryByteStream(article)

    def migrateToAnotherSite(self, anotherSite, pos):
        client = MongoClient(anotherSite)
        u_col = [self.user_one_col, self.user_two_col]
        a_col = [self.art_one_col, self.art_two_col]
        r_col = [self.read_one_col, self.read_two_col]
        pop_col = [self.pop_one_col, self.pop_two_col]
        be_col = [self.beread_one_col, self.beread_two_col]

        db = client['proj']
        db[f'User-DBMS-Copy-{pos + 1}'].drop()
        mongoDBInserter.insert_one_collection_raw_data(
            db[f'User-DBMS-Copy-{pos + 1}'], u_col[pos], self.USER_BULK_LIMIT)
        print("finish migrating User table")

        db[f'Article-DBMS-Copy-{pos + 1}'].drop()
        mongoDBInserter.insert_one_collection_raw_data(
            db[f'Article-DBMS-Copy-{pos + 1}'], a_col[pos], self.ARTICLE_BULK_LIMIT)
        print("finish migrating Article table")

        db[f'Read-DBMS-Copy-{pos + 1}'].drop()
        mongoDBInserter.insert_one_collection_raw_data(
            db[f'Read-DBMS-Copy-{pos + 1}'], r_col[pos], self.READ_BULK_LIMIT)
        print("finish migrating Read table")

        db[f'Be-Read-DBMS-Copy-{pos + 1}'].drop()
        mongoDBInserter.insert_one_collection_raw_data(
            db[f'Be-Read-DBMS-Copy-{pos + 1}'], be_col[pos], 60000)
        print("finish migrating Be-Read table")

        db[f'Popular-Rank-DBMS-Copy-{pos + 1}'].drop()
        mongoDBInserter.insert_one_collection_raw_data(
            db[f'Popular-Rank-DBMS-Copy-{pos + 1}'], pop_col[pos], 10)
        print("finish migrating Popular-Rank table")
        print("finish data migration")
        self.siteOneClient = client
        self.refreshSite()
        self.check_DBMS_status(True)
        print(
            f"please check server {anotherSite}, data has migrated to this server")


mongoDBManager = MongoDBManager(False)
