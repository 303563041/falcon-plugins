#!/usr/bin/env python

import time
import datetime
import sys
import os
import json
import re


def checkFile(configs):
    '''
    check log
    '''
    config = "/data/open-falcon/cfg.json"
    with open(config) as cfg:
        data = json.load(cfg)
    hostname = data["hostname"]
    rs = []

    for config in configs:
        path = config["path"]
        if not os.path.isfile(path):
            continue

        curOffset = os.path.getsize(path)
        if curOffset < 0:
            continue

        for keyword in config["keywords"]:
            exp = keyword["exp"]
            tag = keyword["tag"] + "=" + exp
            metric = "logs-" + os.path.basename(path)
            task = metric + "-" + exp
            index = 0
            preOffset = loadOffset(task)
            if preOffset < 0:
                saveOffset(task, curOffset)
                continue

            saveOffset(task, curOffset)

            if preOffset > curOffset:
                preOffset = curOffset

            file = open(path, "r")
            file.seek(preOffset)
            lines = file.readlines()
            file.close()
            for line in lines:
                if re.search(exp, line):
                    index += 1
            value = index
            t = {}
            t['endpoint'] = hostname
            t['timestamp'] = int(time.time())
            t['step'] = 60
            t['counterType'] = "GAUGE"
            t['metric'] = metric
            t['tags'] = tag
            t['value'] = value
            rs.append(t)
    print json.dumps(rs)


def main():
    try:
        f = open("/data/open-falcon/plugin/log/config/cfg.json", "r")
        configs = json.loads(f.read())
    except Exception as e:
        print "here", e
        sys.exit()
    f.close()
    if not os.path.exists("/data/open-falcon/plugin/log/data"):
        os.makedirs("/data/open-falcon/plugin/log/data")

    checkFile(configs)


def loadOffset(task):
    fileName = task + ".logoffset"
    fullpath = "/data/open-falcon/plugin/log/data/" + fileName

    if not os.path.isfile(fullpath):
        return -1

    file = open(fullpath, 'r')
    offset = long(file.readline().split()[0])
    file.close()
    return offset


def saveOffset(task, offset):
    fileName = task + ".logoffset"
    fullpath = "/data/open-falcon/plugin/log/data/" + fileName

    file = open(fullpath, 'w')
    file.write(str(offset))
    file.close()
    return


if __name__ == "__main__":
    main()
