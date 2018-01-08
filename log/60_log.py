#!/usr/bin/env python

import time
import datetime
import sys
import os
import json

def checkFile(configs):
    '''
    check log
    '''
    for config in configs:
        path = config["path"]
        if not os.path.isfile(path):
            return 0

        curOffset = os.path.getsize(path)
        if curOffset < 0:
            return 0

        for keyword in config["keywords"]:
            exp = keyword["exp"]
            tag = keyword["tag"] + "=" + exp
            metric = "logs"
            index = 0
            preOffset = loadOffset(exp)
            if preOffset < 0:
                saveOffset(exp, curOffset)
                return 0

            saveOffset(exp, curOffset)

            if preOffset > curOffset:
                preOffset = curOffset

            file = open(path, "r")
            file.seek(preOffset)
            lines = file.readlines()
            for line in lines:
                if re.search(exp, line):
                    index += 1
            value = index
            sendMsg(metric, tag, value)


def sendMsg(metric, tag, value):
    '''
    send content to falcon api
    '''
    config = "/data/open-falcon/cfg.json"
    with open(config) as cfg:
        data = json.load(cfg)
    hostname = data["hostname"]
    t = {}
    t['endpoint'] = hostname
    t['timestamp'] = int(time.time())
    t['step'] = 60
    t['counterType'] = "GAUGE"
    t['metric'] = metric
    t['tags'] = tag
    t['value'] = value
    print json.dumps(t)

def main():
    try:
        f = open("./config/cfg.json", "r")
        configs = json.loads(f.read())
    except Exception as e:
        print "here", e
        sys.exit()
    f.close()
    if not os.path.exists("./data"):
        os.makedirs("./data")
        
    checkFile(configs)

def loadOffset(exp):
    fileName = exp + ".logoffset"
    fullpath = "./data/" + fileName

    if not os.path.isfile(fullpath):
        return -1;

    file = open(fullpath, 'r')
    offset = long(file.readline().split()[0])
    file.close()
    return offset


def saveOffset(exp, offset):
    fileName = exp + ".logoffset"
    fullpath = "./data/" + fileName

    file = open(fullpath, 'w')
    file.write(str(offset))
    file.close()
    return

if __name__ == "__main__":
    main()