import time
from mongo_readraw import *
from be_read import timer
from new_server import *
from printer import Printer

data_migration_port = 5100
new_site_addr = f"mongodb://127.0.0.1:{data_migration_port}"
timer.start()
with dataMigration_demo(data_migration_port) as p:
    mongoDBManager.migrateToAnotherSite(f"{new_site_addr}", 0)
    client = MongoClient(f"{new_site_addr}")
    subprocess.run(['mongo', '--port', str(data_migration_port), 'admin', '--eval', 'db.shutdownServer()'],
                   shell=True,
                   stdout=subprocess.DEVNULL,
                   stderr=subprocess.STDOUT)
    p.kill()

timer.recordAndPrint(
    lambda: True,
    f"Data finish migrating to another site: [{new_site_addr}]"
)
print(
    f"You can check it in the mongo shell. However, migrated server is closed currently to prevent resources leak, you can restart this server with command 'mongod --port {data_migration_port} --dbpath {os.getcwd() + os.sep + 'data_migration_1'} --noauth' and check it in mongo shell with command 'mongo --port {data_migration_port}'")
