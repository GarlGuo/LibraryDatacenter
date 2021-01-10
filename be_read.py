from redis_util import getAIDPos
import numpy as np
import time
import pymongo
import time


class TimeRecorder:

    def __init__(self):
        self.prevT = 0

    def start(self):
        self.prevT = time.time()

    def recordAndPrint(self, pred, msg=None):
        if pred():
            if msg:
                print(msg)
            print(f"Time delta: {time.time() - self.prevT}")
            prevT = time.time()

    def stop(self, quiet=False):
        if quiet == False:
            print(f"Last time: {time.time() - self.prevT}")
        prevT = 0


timer = TimeRecorder()

COUNT = 0


class Be_read_Insert_Handler:
    BE_READ_BULK_LIMIT = 60000

    def __init__(self, maxNum, BE_READ_COL):
        self.readUID_count = 0
        self.readUID_LIST = np.ndarray(maxNum, dtype='<U64')
        self.agreeUID_count = 0
        self.agreeUID_LIST = np.ndarray(maxNum, dtype='<U64')
        self.commentUID_count = 0
        self.commentUID_LIST = np.ndarray(maxNum, dtype='<U64')
        self.shareUID_count = 0
        self.shareUID_LIST = np.ndarray(maxNum, dtype='<U64')
        self.bulk_ptr = 0
        self.bulk_write_cache = [0 for _ in range(self.BE_READ_BULK_LIMIT)]
        self.BE_READ_COL = BE_READ_COL

    def startNewArticle(self, aid):
        self.aid = aid

    def read_row_from_read_col(self, row):
        user = row['uid']
        if row['readOrNot'] == '1':
            self.readUID_LIST.itemset(self.readUID_count, user)
            self.readUID_count += 1

        if row['agreeOrNot'] == '1':
            self.agreeUID_LIST.itemset(self.agreeUID_count, user)
            self.agreeUID_count += 1

        if row['shareOrNot'] == '1':
            self.shareUID_LIST.itemset(self.shareUID_count, user)
            self.shareUID_count += 1

        if row['commentOrNot'] == '1':
            self.commentUID_LIST.itemset(self.commentUID_count, user)
            self.commentUID_count += 1

    def finalWrite(self):
        self.BE_READ_COL.bulk_write(
            self.bulk_write_cache[: self.bulk_ptr], ordered=True)

    def onFinishOneArticle(self):
        global COUNT
        data = pymongo.InsertOne({
            'id': COUNT,
            'timestamp': time.time() * 1000,
            'aid': self.aid,
            'readNum': self.readUID_count,
            'readUidList': self.readUID_LIST[: self.readUID_count].tolist(),
            'commentNum': self.commentUID_count,
            'commentUidList': self.commentUID_LIST[: self.commentUID_count].tolist(),
            'agreeNum': self.agreeUID_count,
            'agreeUidList': self.agreeUID_LIST[: self.agreeUID_count].tolist(),
            'shareNum': self.shareUID_count,
            'shareUidList': self.shareUID_LIST[: self.shareUID_count].tolist(),
        })
        if self.bulk_ptr == self.BE_READ_BULK_LIMIT:
            self.BE_READ_COL.bulk_write(
                self.bulk_write_cache, ordered=True)
            self.bulk_ptr = 0
        self.bulk_write_cache[self.bulk_ptr] = data
        self.bulk_ptr += 1
        COUNT += 1
        self.readUID_count = 0
        self.agreeUID_count = 0
        self.shareUID_count = 0
        self.commentUID_count = 0


def aid_to_int(row):
    return int(row['aid'])


def beread_insert(read_iter,
                  manager,
                  be_read_col_one,
                  be_read_col_two):
    be_read_col_one.drop()
    be_read_col_two.drop()
    read_col_ptr, MAX_NUM = 0, len(read_iter)
    handler_list = [Be_read_Insert_Handler(
        MAX_NUM, be_read_col_one
    ),
        Be_read_Insert_Handler(
            MAX_NUM, be_read_col_two
    )]
    read_iter = list(read_iter)
    read_iter.sort(key=aid_to_int)
    while read_col_ptr < MAX_NUM:
        one_aid = read_iter[read_col_ptr]['aid']
        pos = getAIDPos(one_aid, manager)
        handler_list[pos].startNewArticle(one_aid)
        while (read_col_ptr < MAX_NUM and
               one_aid == read_iter[read_col_ptr]['aid']):
            handler_list[pos].read_row_from_read_col(
                read_iter[read_col_ptr]
            )
            read_col_ptr += 1
        handler_list[pos].onFinishOneArticle()
    handler_list[0].finalWrite()
    handler_list[1].finalWrite()
