from pymongo import MongoClient
from mongo_readraw import mongoDBManager
from hdfs_file_util import removedir
import subprocess
import os
import time

def runServer(port, dbpath, replicaName):
    return subprocess.Popen(['mongod', '--port', str(port),
                             '--dbpath', dbpath, '--noauth', '--replSet', replicaName,
                             '--shardsvr'],
                            shell=True,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.STDOUT)


def runServer_demo(port):
    dir = os.getcwd() + os.sep + 'new_server'
    if (os.path.exists(dir)):
        removedir(dir)
    else:
        os.mkdir(dir)
    abs_dir = os.path.abspath(dir)
    return runServer(port, abs_dir, 'proj')


def addServer(serverName, running_primary):
    client = MongoClient(serverName)
    conf = running_primary.admin.command({'replSetGetConfig': 1})
    conf['config']['members'].append({
        '_id': len(conf['config']['members']),
        'host': serverName,
        'priority': 0,
        'votes': 0
    })
    conf['config']['version'] += 1
    print(running_primary.admin.command(
        {'replSetReconfig': conf['config']}, {'force': True}))


def dataMigration_demo(port):
    dir1 = os.getcwd() + os.sep + 'data_migration_1'
    if (os.path.exists(dir1)):
        removedir(dir1)
    else:
        os.mkdir(dir1)
    abs_dir = os.path.abspath(dir1)
    commands = ['mongod', '--port', str(port),
                '--dbpath', abs_dir, '--noauth']
    return subprocess.Popen(commands,
                            shell=True,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.STDOUT)

