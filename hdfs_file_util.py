from pathlib import Path
import os
import subprocess
import redis
import json
import time
from be_read import timer
from redis_util import redis_server, redisStoreFiles, redisRemoveFiles, fileKeyInRedis
import os
import shutil


LOCAL_DIRECTORY = os.getcwd() + os.path.sep + 'hdfs_file_cache'


def clearFileCache():
    if os.path.exists(LOCAL_DIRECTORY):
        removedir(LOCAL_DIRECTORY)
    else:
        os.mkdir(LOCAL_DIRECTORY)
    print("file cache cleared!")


def removedir(directory):
    directory = Path(directory)
    for item in directory.iterdir():
        if item.is_dir():
            removedir(item)
            os.rmdir(item)
        else:
            item.unlink()


def checkFileExistance(proposed_directory, fileName):
    return os.path.isfile(LOCAL_DIRECTORY + os.sep +
                          proposed_directory + os.sep + fileName)


def recursiveStoreFileInRedis(dirNeedToDownload):
    if type(dirNeedToDownload) == str:
        dirNeedToDownload = [dirNeedToDownload]
    finalFiles = []
    for dir in dirNeedToDownload:
        local_dir = LOCAL_DIRECTORY + os.sep + dir
        for item in Path(local_dir).iterdir():
            name = os.path.basename(item)
            finalFiles.append(dir + name)
    redisStoreFiles(finalFiles)


def uploadFiles(localDirs):
    for d in localDirs:
        subprocess.run(['hdfs', 'dfs', '-put', '-f', (os.getcwd() + os.sep + d + os.sep + "*").replace("\\", "/"), os.path.basename(d)],
                       shell=True,
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.STDOUT)
        print(f"Directory {d} has been uploaded")


def fetchFiles(fileNames, remoteFileAddrs, remoteDirs):
    commands = ['hdfs', 'dfs', '-get', '-p', '-f']
    dirNeedToDownload = set()
    for i, oneFileAddr in enumerate(remoteFileAddrs):
        if not fileKeyInRedis(oneFileAddr):
            if not checkFileExistance(remoteDirs[i], fileNames[i]):
                dirNeedToDownload.add(remoteDirs[i])
            else:
                recursiveStoreFileInRedis(remoteDirs[i])
    if len(dirNeedToDownload) == 0:
        return
    for oneDir in dirNeedToDownload:
        commands.append(oneDir)
    commands.append(LOCAL_DIRECTORY)
    isSuccess = subprocess.run(commands,
                               shell=True,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.STDOUT).returncode
    recursiveStoreFileInRedis(dirNeedToDownload)


def getOneArticleByteStream(fileAddrCollection, pop_rank_num, granularity, delim=','):
    videoCollection = fileAddrCollection['video']
    textCollection = fileAddrCollection['text']
    imageCollection = fileAddrCollection['image']

    fileNames, remoteAddrs, remoteDirs = [], [], []
    for i, oneFile in enumerate(imageCollection.split(delim)):
        if not oneFile:
            continue
        imgDirectory_str = oneFile[7:]  # image_a
        imgDirectory_str = imgDirectory_str[: imgDirectory_str.index('_')]
        remoteDirectory = "article" + imgDirectory_str + os.sep
        fileNames += [oneFile]
        remoteAddrs += [remoteDirectory + oneFile]
        remoteDirs += [remoteDirectory]

    for i, oneFile in enumerate(textCollection.split(delim)):
        if not oneFile:
            continue
        textDirectory_str = oneFile[6:]  # text_a
        textDirectory_str = textDirectory_str[: textDirectory_str.index('.')]
        remoteDirectory = "article" + textDirectory_str + os.sep
        fileNames += [oneFile]
        remoteAddrs += [remoteDirectory + oneFile]
        remoteDirs += [remoteDirectory]

    for i, oneFile in enumerate(videoCollection.split(delim)):
        if not oneFile:
            continue
        videoDirectory_str = oneFile[7:]  # video_a
        videoDirectory_str = videoDirectory_str[: videoDirectory_str.index(
            '_')]
        remoteDirectory = "article" + videoDirectory_str + os.sep
        fileNames += [oneFile]
        remoteAddrs += [remoteDirectory + oneFile]
        remoteDirs += [remoteDirectory]

    fetchFiles(fileNames, remoteAddrs, remoteDirs)
    new_dir = f"pop_rank_article_detail_{pop_rank_num}"
    if os.path.exists(new_dir) == False:
        os.mkdir(new_dir)
    for i, oneFile in enumerate(remoteAddrs):
        local_file_addr = LOCAL_DIRECTORY + os.path.sep + oneFile
        correspond_dir = new_dir + os.path.sep + granularity + \
            os.sep + fileAddrCollection['aid'] + os.sep
        if os.path.exists(correspond_dir) == False:
            os.makedirs(correspond_dir)
        pop_rank_addr = correspond_dir + fileNames[i]
        shutil.copyfile(local_file_addr, pop_rank_addr)
    print(
        f"Finish downloading for pop_rank_{pop_rank_num} with granularity: {granularity}. Article id: {fileAddrCollection['aid']}")


def uploadDirectoryByteStream(fileAddrCollection, delim=','):
    videoCollection = fileAddrCollection['video']
    textCollection = fileAddrCollection['text']
    imageCollection = fileAddrCollection['image']

    to_upload = set()
    for i, oneFile in enumerate(imageCollection.split(delim)):
        if not oneFile:
            continue
        imgDirectory_str = oneFile[7:]  # image_a
        imgDirectory_str = imgDirectory_str[: imgDirectory_str.index('_')]
        localDir = "db" + os.sep + "articles" + os.sep + "article" + \
            imgDirectory_str
        to_upload.add(localDir)

    for i, oneFile in enumerate(textCollection.split(delim)):
        if not oneFile:
            continue
        textDirectory_str = oneFile[6:]  # text_a
        textDirectory_str = textDirectory_str[: textDirectory_str.index('.')]
        localDir = "db" + os.sep + "articles" + os.sep + \
            "article" + textDirectory_str
        to_upload.add(localDir)

    for i, oneFile in enumerate(videoCollection.split(delim)):
        if not oneFile:
            continue
        videoDirectory_str = oneFile[7:]  # video_a
        videoDirectory_str = videoDirectory_str[: videoDirectory_str.index(
            '_')]
        localDir = "db" + os.sep + "articles" + os.sep + \
            "article" + videoDirectory_str
        to_upload.add(localDir)

    uploadFiles(to_upload)
