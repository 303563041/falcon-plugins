#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import json
import requests
import time

def get_statistics(host, port):
    ''' get open falcon statistics '''
    uri = "/counter/all"
    url = host + ":" + str(port) + uri
    try:
        response = requests.get(url)
    except Exception as e:
        print "Url request error: {0}".format(e)

    return json.loads(response.text)

def main():
    p = []
    ts = int(time.time())
    metrics = [
        {
            'name': 'transfer',
            'port': 6060,
            'host': 'http://localhost',
            'qps': ['RecvCnt', 'SendToGraphCnt', 'SendToJudgeCnt']
        },
        {
            'name': 'graph',
            'port': 6071,
            'host': 'http://localhost',
            'qps': ['GraphRpcRecvCnt', 'GraphQueryCnt']
        }
    ]

    for metric in metrics:
        name = metric["name"]
        host = metric["host"]
        port = metric["port"]
        qps = metric["qps"]

        response = get_statistics(host, port)
        if name == "transfer":
            if response["msg"] == "success":
                for m in qps:
                    for i in response["data"]:
                        if i["Name"] == m:
                            value = i["Qps"]
                    datapoint = {
                        'metric': '{0}.{1}.Qps'.format(name, m),
                        'endpoint': '172.31.32.191-open-falcon',
                        'timestamp': ts,
                        'step': '60',
                        'value': value,
                        'counterType': 'COUNTER',
                        'tags': ''
                    }
                    p.append(datapoint)
            else:
                value = 0
                for m in qps:
                    datapoint = {
                        'metric': '{0}.{1}.Qps'.format(name, m),
                        'endpoint': '172.31.32.191-open-falcon',
                        'timestamp': ts,
                        'step': '60',
                        'value': value,
                        'counterType': 'COUNTER',
                        'tags': ''
                    }
                    p.append(datapoint)
        elif name == "graph":
            if response:
                for m in qps:
                    for i in response:
                        if i["Name"] == m:
                            value = i["Qps"]
                    datapoint = {
                        'metric': '{0}.{1}.Qps'.format(name, m),
                        'endpoint': '172.31.32.191-open-falcon',
                        'timestamp': ts,
                        'step': '60',
                        'value': value,
                        'counterType': 'COUNTER',
                        'tags': ''
                    }
                    p.append(datapoint)
            else:
                value = 0
                for m in qps:
                    datapoint = {
                        'metric': '{0}.{1}.Qps'.format(name, m),
                        'endpoint': '172.31.32.191-open-falcon',
                        'timestamp': ts,
                        'step': '60',
                        'value': value,
                        'counterType': 'COUNTER',
                        'tags': ''
                    }
                    p.append(datapoint)

    print json.dumps(p)

if __name__ == "__main__":
    main()

