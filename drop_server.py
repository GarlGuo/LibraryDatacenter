import mongo_readraw
from mongo_readraw import mongoDBManager
from pymongo import MongoClient
from query import *
import subprocess


secondary_node = mongoDBManager.siteOneSecondaryList[0]
secondary_node = secondary_node[secondary_node.rfind(":") + 1:]
subprocess.run(['mongo', '--port', secondary_node, 'admin', '--eval', 'db.shutdownServer()'],
               shell=True,
               stdout=subprocess.DEVNULL,
               stderr=subprocess.STDOUT)
print(f"we have dropped the server 'localhost:{secondary_node}'")
print("Let's do some queries to verify it is still working")

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

print("Let's do one insert")
mongoDBManager.siteOneClient['Test']['TestCol'].insert({"test insert": 1})
print("Insertion succeed!!!")
print("Our database still works!!!")
