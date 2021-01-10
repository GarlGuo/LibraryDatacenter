from mongo_readraw import mongoDBManager
from new_server import *
from server_status import checkServerAlive

print("\n\n")
print("Before adding new server")
print("\n")
host_port = 5413
mongoDBManager.check_DBMS_status()

with runServer_demo(host_port) as p:
    addServer(f"localhost:{host_port}", mongoDBManager.siteOneClient)
    print("\n\n")
    print(f"After adding new server: [localhost:{host_port}]")
    print("\n")
    time.sleep(3)
    mongoDBManager.check_DBMS_status(False)
    print('\n\n')
    print(
        f"You can check it in the mongo shell. However, migrated server is closed currently to prevent resources leak, you can restart this server with command 'mongod --port {host_port} --dbpath {os.getcwd() + os.sep + 'new_server'} --noauth' and check it in mongo shell with command 'mongo --port {host_port}'")
