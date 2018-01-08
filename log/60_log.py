#!/usr/bin/env python

import time
import datetime
import sys
import os
import json

def checkFile(task):
    instanceid = ""

    try:
        with open ("/var/info/instanceid", "r") as myfile:
            instanceid = myfile.read().replace('\n', '')
    except Exception,e:
        return

    """
    example:

    logPath = "/mnt/logs/mysql/mysql-%Y-%-m-%-d.log" #/mnt/logs/mysql/mysql-2017-4-1.log
    logPath = "/mnt/logs/mysql/mysql-%Y-%m-%d.log"   #/mnt/logs/mysql/mysql-2017-04-01.log

    strftime format codes:
       %Y   Year with century as a decimal number.                  1970, 1988, 2001, 2013
       %y   Year without century as a zero-padded decimal number.   00, 01, ..., 99
       %m   Month as a zero-padded decimal number.                  01, 02, ..., 12
       %d   Day of the month as a zero-padded decimal number.       01, 02, ..., 31
       %H   Hour (24-hour clock) as a zero-padded decimal number.   00, 01, ..., 23

       _    (underscore) Pad a numeric result string with spaces.
       -    (dash) Do not pad a numeric result string.
       0    Pad a numeric result string with zeros even if the conversion specifier character uses space-padding by default.

       http://man7.org/linux/man-pages/man3/strftime.3.html
    """
    try:
        task["logPath"] = datetime.datetime.now().strftime(task["logPath"])
    except Exception,e:
        return

    if not os.path.isfile(task["logPath"]):
        return 0

    curOffset = os.path.getsize(task["logPath"])
    if curOffset < 0:
        return 0

    preOffset = loadOffset(task)
    if preOffset < 0:
        saveOffset(task, curOffset)
        return 0

    saveOffset(task, curOffset)

    if preOffset > curOffset:
        preOffset = curOffset

    file = open(task["logPath"], "r")
    file.seek(preOffset)


    index = 0
    content = ""

    while index < 50:
        line = file.readline()
        if not line:
            break
        if len(line) > 512:
            line = line[:511]
        content += line
        content += "\n"
        index += 1

    if index == 0:
        index = 1

    # compose message body
    body = {}
    body["product"] = task["project"]
    body["group"]   = task["release"]
    body["environment"] = task["environment"]
    body["service"] = task["service"]
    body["module"] = task["module"]
    body["logPath"] = task["logPath"]
    body["namespace"] = instanceid
    body["message"] = content

    content = json.dumps(body)
    try:
        sendMsg(content, index)
    except Exception,e:
        return

def main():
    try:
        tasks = loadConfig("/usr/lib64/collector-fluentd/plugins_v2/config")
        if len(tasks) == 0:
            sys.exit()
    except Exception as e:
        print "here", e
        sys.exit()

    threads = []
    ts = time.time()
    for task in tasks:
        threadHandle = Thread(target=checkFile,  args=(task, ))
        threadHandle.start()
        threads.append(threadHandle)

    for handle in threads:
        handle.join()

def loadOffset(task):
    fileName = "idp.logoffset"
    fullpath = "/usr/lib64/collector-fluentd/plugins_v2/data/" + fileName

    if not os.path.isfile(fullpath):
        return -1;

    file = open(fullpath, 'r')
    offset = long(file.readline().split()[0])
    file.close()
    return offset


def saveOffset(task, offset):
    fileName = task["project"] + "-" + task["release"] + "-" + task["environment"] + "-" + task["service"] + "-" + task[
        "module"] + "-" + task["name"] + ".logoffset"
    fullpath = "/usr/lib64/collector-fluentd/plugins_v2/data/" + fileName

    file = open(fullpath, 'w')
    file.write(str(offset))
    file.close()
    return

if __name__ == "__main__":
    main()