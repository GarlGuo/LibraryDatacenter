import redis
redis_server = redis.Redis()


def clearRedisCache():
    redis_server.delete("Files", "UID0", "UID1", "AID0", "AID1")
    print("redis cache cleared!")


def redisStoreFiles(col_or_singleton):
    with redis_server.pipeline() as p:
        if type(col_or_singleton) in [list, tuple]:
            for elem in col_or_singleton:
                p.sadd("Files", elem.replace(
                    '\\', ' ').replace('/', ' '))
        elif type(col_or_singleton) == str:
            p.sadd("Files", col_or_singleton.replace(
                '\\', ' ').replace('/', ' '))
        else:
            raise TypeError(type(col_or_singleton))
        p.execute()


def redisRemoveFiles(*files):
    for oneFile in files:
        redis_server.srem("Files", oneFile.replace(
            '/', ' ').replace('\\', ' '))


def fileKeyInRedis(oneFile):
    return redis_server.sismember("Files", oneFile.replace(
        '/', ' ').replace('\\', ' '))


def storeAIDList(pos, id_lst):
    if pos == 0:
        redis_server.sadd('AID0', *id_lst)
    else:
        redis_server.sadd('AID1', *id_lst)


def storeUIDList(pos, id_lst):
    if pos == 0:
        redis_server.sadd('UID0', *id_lst)
    else:
        redis_server.sadd('UID1', *id_lst)


def getUIDPos(uid, mongoDBManager):
    if redis_server.sismember("UID0", uid):
        return 0
    elif redis_server.sismember("UID1", uid):
        return 1
    elif mongoDBManager.user_one_col.find_one({'uid': uid}):
        redis_server.sadd("UID0", uid)
        return 0
    else:
        redis_server.sadd("UID1", uid)
        return 1


def getAIDPos(aid, mongoDBManager):
    if redis_server.sismember("AID0", aid):
        return 0
    elif redis_server.sismember("AID1", aid):
        return 1
    elif mongoDBManager.art_one_col.find_one({'aid': aid}):
        redis_server.sadd("AID0", aid)
        return 0
    else:
        redis_server.sadd("AID1", aid)
        return 1


def getAID_in_one():
    return redis_server.smembers("AID0")


def getAID_in_two():
    return redis_server.smembers("AID1")
