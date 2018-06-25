#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json
import time
from multiprocessing.pool import ThreadPool as Pool
import logging

class Resource():

    def __init__(self):
        self.cloudwatch_client = boto3.client("cloudwatch")
        self.ts = int(time.time())
        self.start_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 180))
        self.end_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 120))
        self.p = []
        self.step = 60
        self.counterType = "GAUGE"
        self.dynamodb_metric_list = [
            "ConsumedWriteCapacityUnits"
        ]

    def getDynamodbMonitor(self, metric):
        """
        get dynamodb monitor data from cloud watch
        """
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                    Namespace = 'AWS/DynamoDB',
                    MetricName = '{0}'.format(metric),
                    Dimensions = [
                        {
                            'Name': 'TableName',
                            'Value': 'selvashubLog'
                        },
                    ],
                    StartTime = self.start_time,
                    EndTime = self.end_time,
                    Period = 60,
                    Statistics = ['Sum']
                )
            if metric == "ProvisionedWriteCapacityUnits" or metric == "ProvisionedReadCapacityUnits":
                value = response["Datapoints"][0]["Sum"]
            else:
                value = response["Datapoints"][0]["Sum"] / 60 # per seconde
        except Exception, e:
            value = -1

        i = {
            'metric': 'dynamodb.r{0}'.format(metric),
            'endpoint': 'Townkins-dynamodb-selvashubLog',
            'timestamp': self.ts,
            'step': self.step,
            'value': value,
            'counterType': self.counterType,
            'tags': ""
        }
        self.p.append(i)

    def run(self):

        # create thread pool
        pool = Pool(10)

        # main task
        for metric in self.dynamodb_metric_list:
            try:
                self.getDynamodbMonitor(metric)
            except Exception,e:
                logging.error(e)
                continue
        pool.close()
        pool.join()

        print json.dumps(self.p)

if __name__ == "__main__":
    Resource().run()
