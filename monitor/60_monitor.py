#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import json
import requests
import time

class Resource:

    def __init__(self):
        self.p = []
        self.ts = int(time.time())
        self.step = 60
        self.metrics = [
            ("transfer", "http://localhost", "6060", "RecvCnt"),
            ("transfer", "http://localhost", "6060", "SendToGraphCnt"),
            ("transfer", "http://localhost", "6060", "SendToJudgeCnt"),
            ("graph", "http://localhost", "6071", "GraphRpcRecvCnt"),
            ("graph", "http://localhost", "6071", "GraphQueryCnt")
        ]


    def get_statistics(self, host, port):
        ''' get open falcon statistics '''
        uri = "/counter/all"
        url = host + ":" + port + uri
        try:
            response = requests.get(url)
        except Exception as e:
            print "Url request error: {0}".format(e)

        return json.loads(response.text)

    def run(self):
        for metric in self.metrics:
            name = metric[0]
            host = metric[1]
            port = metric[2]
            qps = metric[3]

            response = self.get_statistics(host, port)
            if name == "transfer":
                if response["msg"] == "success":
                    for i in response["data"]:
                        if i["Name"] == qps:
                            value = i["Qps"]
                    datapoint = {
                        'metric': '{0}.a{1}'.format(name, qps),
                        'endpoint': '172.31.32.191-open-falcon',
                        'timestamp': self.ts,
                        'step': self.step,
                        'value': value,
                        'counterType': 'GAUGE',
                        'tags': ''
                    }
                    self.p.append(datapoint)
                else:
                    value = 0
                    datapoint = {
                        'metric': '{0}.a{1}'.format(name, qps),
                        'endpoint': '172.31.32.191-open-falcon',
                        'timestamp': self.ts,
                        'step': self.step,
                        'value': value,
                        'counterType': 'GAUGE',
                        'tags': ''
                    }
                    self.p.append(datapoint)
            elif name == "graph":
                if response:
                    for i in response:
                        if i["Name"] == qps:
                            value = i["Qps"]
                    datapoint = {
                        'metric': '{0}.a{1}'.format(name, qps),
                        'endpoint': '172.31.32.191-open-falcon',
                        'timestamp': self.ts,
                        'step': self.step,
                        'value': value,
                        'counterType': 'GAUGE',
                        'tags': ''
                    }
                    self.p.append(datapoint)
                else:
                    value = 0
                    datapoint = {
                        'metric': '{0}.a{1}'.format(name, qps),
                        'endpoint': '172.31.32.191-open-falcon',
                        'timestamp': self.ts,
                        'step': self.step,
                        'value': value,
                        'counterType': 'GAUGE',
                        'tags': ''
                    }
                    self.p.append(datapoint)

        print json.dumps(self.p)

if __name__ == "__main__":
    Resource().run()

