import time
import pymongo
from mongo_readraw import mongoDBManager
from redis_util import redis_server
import numpy as np
from be_read import aid_to_int
import random


def id(*x): return x


def pipe(data, op=id, parallelizable=True):
    if type(data) in [tuple, list]:
        if parallelizable:
            return op(data[0]), op(data[1])
        else:
            c1, c2 = 0, 0
            if type(data[0]) == pymongo.Cursor:
                c1 = data[0].count()
                c2 = data[1].count()
            else:
                c1 = len(data[0])
                c2 = len(data[1])
            intermediate = np.zeros(c1 + c2, dtype=dict)
            intermediate[:c1] = np.array(list(data[0].find()))
            intermediate[c1:] = np.array(list(data[1].find()))
            return op(intermediate)
    else:
        return op(data)


def sort_op(data, keyFunc, reverse=False):
    return pipe(data, op=lambda x: sorted(x, key=keyFunc, reverse=reverse), parallelizable=False)


def filter_op(data, cond):
    return pipe(data, op=cond, parallelizable=True)


def toList(data):
    if type(data) in [tuple, list] and len(data) == 2:
        return list(data[0]) + list(data[1])
    else:
        return list(data)


def count(data):
    if type(data) in [tuple, list] and len(data) == 2:
        l_len = len(list(data[0]))
        r_len = len(list(data[1]))
        return l_len + r_len

    else:
        return len(list(data))


def limit(data, num):
    if type(data) in [tuple, list] and len(data) == 2:
        arr1 = toList(data[0])
        arr2 = toList(data[1])
        split = int(random.uniform(num * 0.3, num * .7))
        return np.random.choice(data[0], split).tolist() + \
            np.random.choice(data[1], num - split).tolist()
    else:
        return np.random.choice(data, num).tolist()


def uid_to_int(r): return int(r['uid'])


def ALL_PASS(): return True

# left join


def generalJoin(l_col, r_col, key, process_one_entry,
                left_filt=ALL_PASS, right_filt=ALL_PASS):

    l_col = sorted(filter(left_filt, list(l_col)), key=lambda x: x[key])
    l_len = len(l_col)
    r_col = sorted(filter(right_filt, list(r_col)), key=lambda x: x[key])
    r_len = len(r_col)

    l_ptr, r_ptr = 0, 0

    res_ptr, res = 0, np.zeros(max(l_len, r_len),
                               dtype=dict)

    while l_ptr < l_len and r_ptr < r_len:
        while (l_ptr < l_len and
               l_col[l_ptr][key] < r_col[r_ptr][key]):
            l_ptr += 1
        while (l_ptr < l_len and r_ptr < r_len and
               l_col[l_ptr][key] > r_col[r_ptr][key]):
            r_ptr += 1
        while (l_ptr < l_len and r_ptr < r_len and
               l_col[l_ptr][key] == r_col[r_ptr][key]):
            res[res_ptr] = process_one_entry(l_col[l_ptr], r_col[r_ptr])
            res_ptr += 1
            r_ptr += 1
    return res[:res_ptr]


class MongoDBQueryer:

    def __init__(self):
        self.manager = mongoDBManager
        self.cache = redis_server

    def queryUser(self, filt):
        col1 = mongoDBManager.user_one_col.find(filt)
        col2 = mongoDBManager.user_two_col.find(filt)
        return col1, col2

    def queryArticle(self, filt):
        col1 = mongoDBManager.art_one_col.find(filt)
        col2 = mongoDBManager.art_two_col.find(filt)
        return col1, col2

    def queryRead(self, filt):
        col1 = mongoDBManager.user_one_col.find(filt)
        col2 = mongoDBManager.user_two_col.find(filt)
        return col1, col2

    def readJoinWithUser(self, read, user, process,
                         left_filt=ALL_PASS, right_filt=ALL_PASS):
        r1, r2 = read[0], read[1]
        u1, u2 = user[0], user[1]
        res_1 = generalJoin(u1, r1, 'uid', process,
                            left_filt=left_filt, right_filt=right_filt)
        res_2 = generalJoin(u2, r2, 'uid', process,
                            left_filt=left_filt, right_filt=right_filt)
        return res_1, res_2


mongoDBQueryer = MongoDBQueryer()


def extractUidReadAgreeCommentShare(u, r):
    return {
        'user who read (uid)': u['uid'],
        'article read (aid)': r['aid'],
        'user gender': u['gender'],
        'user obtainedCredits': u['obtainedCredits'],
        'agree?': r['agreeOrNot'],
        'share?': r['shareOrNot'],
        'comment?': r['commentOrNot'],
        'commentDetail': r['commentDetail']
    }


def extractAidCategoryRead(a, r):
    return {
        'be-read article (aid)': r['aid'],
        'article title': a['title'],
        'article tag': a['articleTags'],
        'read record uid': r['uid'],
        'article category': a['category'],
        'reader agree?': r['agreeOrNot'],
        'reader share?': r['shareOrNot'],
    }
