from mongo_readraw import *
from hot_articles import rankInserter
from be_read import timer, beread_insert
from query import *
from new_server import *
import threading
import time
import printer


p = printer.Printer("main.py.log")


def ignorePred(): return True


def handleReadForPopRank(col): return col.find({}, {'uid',
                                                    'aid', 'uid',
                                                    'timestamp',
                                                    'readOrNot',
                                                    'agreeOrNot',
                                                    'shareOrNot',
                                                    'commentOrNot'})


print("Let's do several queries with or without joins")

u1, u2 = mongoDBManager.user_one_col, mongoDBManager.user_two_col
a1, a2 = mongoDBManager.art_one_col, mongoDBManager.art_two_col
r1, r2 = mongoDBManager.read_one_col, mongoDBManager.read_two_col

male_res = toList(mongoDBQueryer.queryUser({'gender': 'male'}))
female_res = toList(mongoDBQueryer.queryUser({'gender': 'female'}))
print(
    f"male user total count (count op): {count(male_res)}")
print('\n')
print(
    f"2 male user samples (limit op): {limit(male_res, 2)}")
print('\n')
print(
    f"female user total count (count op): {count(female_res)}")
print('\n')
print(
    f"2 female user samples (limit op): {limit(female_res, 2)}")

print("Let's do a joins")


def user_cond(row): return 1000 < int(row['uid']) < 2000


def art_cond(row): return 10000 < int(row['aid']) < 20000


u_r_join_res = mongoDBQueryer.readJoinWithUser([list(mongoDBManager.read_one_col.find()),
                                                list(mongoDBManager.read_two_col.find())],
                                               [list(mongoDBManager.user_one_col.find()),
                                                list(mongoDBManager.user_two_col.find())],
                                               extractUidReadAgreeCommentShare,
                                               user_cond, user_cond)
u_r_join_num = count(u_r_join_res)
u_r_join_sample_num = 7
u_r_join_sample = limit(u_r_join_res, u_r_join_sample_num)
p.log(
    f"user table joins with read table (a left join) with 1000 < uid < 2000 num: {u_r_join_num}")
p.log(
    f"join sample size {u_r_join_sample_num}: \n" + "\n".join(str(i) for i in u_r_join_sample))

a_r_join_res = generalJoin(mongoDBManager.art_total,
                           mongoDBManager.read_total,
                           'aid',
                           extractAidCategoryRead,
                           art_cond, art_cond)
a_r_join_num = count(a_r_join_res)
a_r_join_sample_num = 10
a_r_join_sample = limit(a_r_join_res, a_r_join_sample_num)
p.log(
    f"article table joins with read table (a left join) with 10000 < aid < 20000 num: {a_r_join_num}")
p.log(
    f"join sample size {a_r_join_sample_num}: \n" + "\n".join(str(i) for i in a_r_join_sample))


p.log("start inserting be-read table")
