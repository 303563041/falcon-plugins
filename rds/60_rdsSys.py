#!/usr/bin/env python
#-*- coding: utf-8 -*-

import boto3
import json
import time
from multiprocessing.pool import ThreadPool as Pool
import logging

class Resource():

    def __init__(self):
        self.cloudwatch_client = boto3.client("cloudwatch")
        self.rdsIdentifiers = []
        self.ts = int(time.time())
        self.start_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 180))
        self.end_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 120))
        self.p = []
        self.step = 60
        self.counterType = "GAUGE"
        self.mysql_metric_list = [
            "FreeableMemory",
            "ReadLatency",
            "ReadThroughput",
            "WriteIOPS",
            "ReadIOPS",
            "FreeStorageSpace",
            "DiskQueueDepth",
            "WriteLatency",
            "DatabaseConnections",
            "NetworkReceiveThroughput",
            "WriteThroughput",
            "NetworkTransmitThroughput",
            "CPUUtilization",
            "SwapUsage",
            "BinLogDiskUsage"
        ]
        self.aurora_metric_list = [
            "Queries",
            "Deadlocks",
            "DatabaseConnections",
            "DeleteThroughput",
            "DMLThroughput",
            "DeleteLatency",
            "CommitThroughput",
            "NetworkThroughput",
            "InsertThroughput",
            "SelectLatency",
            "CPUUtilization",
            "NetworkReceiveThroughput",
            "FreeLocalStorage",
            "FreeableMemory",
            "UpdateLatency",
            "NetworkTransmitThroughput",
            "CommitLatency",
            "LoginFailures",
            "SelectThroughput",
            "UpdateThroughput",
            "DDLThroughput",
            "DDLLatency",
            "DMLLatency",
            "InsertLatency",
            "BinLogDiskUsage"
        ]
        self.memory_mapping = {
            'db.t2.micro': 1,
            'db.t2.small': 2,
            'db.t2.medium': 4,
            'db.r3.large': 15,
            'db.r3.xlarge': 30.5,
            'db.r3.2xlarge': 61,
            'db.r3.4xlarge': 122,
            'db.r3.8xlarge': 244,
            'db.m3.medium': 3.75,
            'db.m3.large': 7.5,
            'db.m3.xlarge': 15,
            'db.m3.2xlarge': 30,
            'db.m4.large': 8,
            'db.m4.xlarge': 16,
            'db.m4.2xlarge': 32,
            'db.m4.4xlarge': 64,
            'db.m4.10xlarge': 160,
            'other': 0
        }

        self.p = []

    def getRdsIdentifiers(self):
        """
        get rds identifier list to self.rdsIdentifiers parameters
        """
        client = boto3.client('rds')
        response = client.describe_db_instances()
        for r in response["DBInstances"]:
            identifier = r["DBInstanceIdentifier"]
            storageType = r["StorageType"]
            DBInstanceClass = r["DBInstanceClass"]
            AllocatedStorage = r["AllocatedStorage"]
            self.rdsIdentifiers.append((identifier, storageType, DBInstanceClass, AllocatedStorage))

    def getRdsMonitor(self, identifier, metric, storageType, DBInstanceClass, AllocatedStorage):
        """
        get per rds metric statistics
        """
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                    Namespace = 'AWS/RDS',
                    MetricName = '{0}'.format(metric),
                    Dimensions = [
                        {
                            'Name': 'DBInstanceIdentifier',
                            'Value': '{0}'.format(identifier)
                        },
                    ],
                    StartTime = self.start_time,
                    EndTime = self.end_time,
                    Period = 60,
                    Statistics = ['Average']
                )
            if metric == 'FreeableMemory':
                memory_total = self.memory_mapping[DBInstanceClass]
                value = response["Datapoints"][0]["Average"]/(memory_total * 1024 * 1024 * 1024) * 100
            elif metric == 'FreeStorageSpace':
                value = response["Datapoints"][0]["Average"]/(AllocatedStorage * 1024 * 1024 * 1024) * 100
            else:
                value = response["Datapoints"][0]["Average"]
        except Exception, e:
            value = -1

        i = {
            'metric': 'rds.r{0}'.format(metric),
            'endpoint': 'Townkins-rds-{0}'.format(identifier),
            'timestamp': self.ts,
            'step': self.step,
            'value': value,
            'counterType': self.counterType,
            'tags': ""
        }
        self.p.append(i)

    def run(self):
        """
        main task
        """
        # get rds identifiers list
        self.getRdsIdentifiers()

        # create thread pool
        pool = Pool(10)

        # main task
        for identifier, storageType, DBInstanceClass, AllocatedStorage in self.rdsIdentifiers:
            if storageType == "aurora":
                metrics = self.aurora_metric_list
            else:
                metrics = self.mysql_metric_list

            for metric in metrics:
                try:
                    pool.apply_async(self.getRdsMonitor, (identifier, metric, storageType, DBInstanceClass, AllocatedStorage))
                except Exception,e:
                    logging.error(e)
                    continue
        pool.close()
        pool.join()

        print json.dumps(self.p)

if __name__ == "__main__":
    Resource().run()
