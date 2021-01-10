from mongo_readraw import mongoDBManager
import time

while 1:
    mongoDBManager.check_DBMS_status()
    time.sleep(5)
