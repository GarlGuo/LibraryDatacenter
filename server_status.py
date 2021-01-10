import sys
import json
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class DBInfoLogger:
    realTime = True
    logger = logging.getLogger('mongo_db.log')

    @staticmethod
    def finishOneItem():
        if DBInfoLogger.realTime:
            print('\n')
        DBInfoLogger.logger.info('\n')

    @staticmethod
    def finishOneSection():
        if DBInfoLogger.realTime:
            print('\n\n')
        DBInfoLogger.logger.info('\n\n')

    @staticmethod
    def printError(e):
        if DBInfoLogger.realTime:
            print(e, file=sys.stderr)
        DBInfoLogger.logger.error(e)

    @staticmethod
    def startPrintNewSite():
        if DBInfoLogger.realTime:
            print('\n\n')
        DBInfoLogger.logger.info('\n\n')

    @staticmethod
    def printSiteLevel(data):
        if DBInfoLogger.realTime:
            print(data)
        DBInfoLogger.logger.info(data)

    @staticmethod
    def printDBLevel(data):
        dbData = '  ' + data
        if DBInfoLogger.realTime:
            print(dbData)
        DBInfoLogger.logger.info(dbData)

    @staticmethod
    def printColLevel(data):
        colData = '    ' + data
        if DBInfoLogger.realTime:
            print(colData)
        DBInfoLogger.logger.info(colData)


def printReplStatus(client):
    repl_info = client.admin.command({'replSetGetStatus': 1})
    DBInfoLogger.printSiteLevel(f'replica set: {repl_info["set"]}')
    for node in repl_info['members']:
        print(f"'{node['name']}': {node['stateStr'].lower()}")
        if len(node['lastHeartbeatMessage']) > 0:
            DBInfoLogger.printDBLevel(
                f"last Heartbeat msg: {node['lastHeartbeatMessage']}")


def checkColInfo(col):
    num = col.count()
    DBInfoLogger.printColLevel(f"docs count: {num}")
    if (num > 0):
        DBInfoLogger.printColLevel(
            f"available keys: {list(col.find_one().keys())}")


def checkWorkLoad(client, dbName):
    db = client[dbName]
    col_list = db.collection_names()
    DBInfoLogger.printDBLevel(
        f"collections in database \"{dbName}\": {col_list}")
    db_stats = db.command("dbstats")
    col_stats = db.command("collstats", "events")
    if 'lastCommittedOpTime' in db_stats:
        lastCommitOpTime = db_stats['lastCommittedOpTime']
    else:
        lastCommitOpTime = None
    totalSize, fsSize = db_stats['totalSize'], db_stats['fsTotalSize']
    DBInfoLogger.printDBLevel(f"obj count: {db_stats['objects']}")
    DBInfoLogger.printDBLevel(f"obj avg size: {db_stats['avgObjSize']:3g}")
    DBInfoLogger.printDBLevel(f"totalSize: {totalSize}")
    DBInfoLogger.printDBLevel(
        f"fsTotalSize: {fsSize}  Usage: {100 * totalSize / fsSize:3f}%")
    if lastCommitOpTime:
        DBInfoLogger.printDBLevel(
            f"lastCommittedOpTime datetime: [{lastCommitOpTime.as_datetime().strftime('%Y/%m/%d, %H:%M:%S.%f')}]  Timestamp: {lastCommitOpTime}")
    # collection info
    for one_col_name in col_list:
        col = client[dbName][one_col_name]
        idx_str = f"index for '{one_col_name}': {col.index_information()}"
        DBInfoLogger.printColLevel(idx_str)
        checkColInfo(col)


def checkReplStatus(hostList):
    connectionSuccessAtLeastOne = False
    for oneHost in hostList:
        try:
            client = MongoClient(host=oneHost)
            client.server_info()  # checkConnection
            if connectionSuccessAtLeastOne == False:
                printReplStatus(client)
                connectionSuccessAtLeastOne = True
        except ConnectionFailure as e:
            print("cannot connect to " + oneHost, file=sys.stderr)
    if connectionSuccessAtLeastOne == False:
        print("cannot retrieve any replica info since NONE of hosts are available", file=sys.stderr)


def checkServerAlive(node):
    try:
        client = MongoClient(host=node)
        client.server_info()  # checkConnection
        return True
    except ConnectionFailure as e:
        return False
