from mongo_readraw import mongoDBManager
import printer
from hot_articles import rankInserter
from be_read import timer, beread_insert

p = printer.Printer("main.py.log")

def handleReadForPopRank(col): return col.find({}, {'uid',
                                                    'aid', 'uid',
                                                    'timestamp',
                                                    'readOrNot',
                                                    'agreeOrNot',
                                                    'shareOrNot',
                                                    'commentOrNot'})


totalRead = mongoDBManager._get_total_with_process(
    mongoDBManager.read_one_col,
    mongoDBManager.read_two_col,
    handleReadForPopRank
)

p.log("start inserting popular rank table")
timer.start()
rankInserter.insertPopularArticles(totalRead,
                                   [mongoDBManager.pop_one_col,
                                       mongoDBManager.pop_two_col],
                                   mongoDBManager.pop_total_col)
timer.recordAndPrint(
    lambda: True, "finish inserting DBMS1 and DBMS2's popular rank")

timer.stop(quiet=True)

print("Let's upload first, please wait....")
mongoDBManager.uploadOnePopularRankFile(mongoDBManager.pop_one_col,
                                        mongoDBManager.art_one_col, "DBMS1")

mongoDBManager.uploadOnePopularRankFile(mongoDBManager.pop_two_col,
                                        mongoDBManager.art_two_col, "DBMS2")

mongoDBManager.refreshSite()
print("start downloading popular-rank articles for DBMS1....")
mongoDBManager.getOnePopularRankFile(
    mongoDBManager.pop_one_col, mongoDBManager.art_one_col, "1")

print("start downloading popular-rank articles for DBMS2....")
mongoDBManager.getOnePopularRankFile(
    mongoDBManager.pop_two_col, mongoDBManager.art_two_col, "2")

print("start downloading popular-rank articles for the entire database....")
mongoDBManager.getTotalPopularRankArticle()

print("Finish download all popular rank article details")


totalRead = mongoDBManager._get_total_with_process(
    mongoDBManager.read_one_col,
    mongoDBManager.read_two_col,
    handleReadForPopRank
)

timer.start()
beread_insert(totalRead,
              mongoDBManager,
              mongoDBManager.beread_one_col,
              mongoDBManager.beread_two_col)
timer.recordAndPrint(
    lambda:  True,
    "finish inserting be_read table"
)
timer.stop(quiet=True)
