#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import commands
import get_rds_instances
from multiprocessing.pool import ThreadPool as Pool 
import logging
import time
import requests

###############
# 
#
###############

class Resource():

    def __init__(self):
        self.step = 60
        self.metric = "rds"
        self.counterType = "GAUGE"
        r = get_rds_instances.GetRdsInstancesList()
        self.rds_instance_identifier = r.get_rds_identifier()
        self.ts = int(time.time())
        self.endpoint = "townkins-rds"
        self.start_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(self.ts - 180))
        self.end_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(self.ts - 120))
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
            "BacktrackStorageSize",
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
        self.p = []

    def get_rds_statistic(self, rdsidentifier):
        """
        get rds monitor data
        """
        try:
            identifier = rdsidentifier.split('\t')[0]
            storageType = rdsidentifier.split('\t')[1]
            #print identifier, storageType
        except Exception,e:
            logging.error(e)

        # get metric statistics
        if storageType == "aurora":
            metric_list = self.aurora_metric_list
        else:
            metric_list = self.mysql_metric_list

        for m in metric_list:
            cmd = "aws cloudwatch get-metric-statistics --metric-name %s --start-time %s --end-time %s --period 60 --namespace AWS/RDS --statistics Average --dimensions Name=DBInstanceIdentifier,Value=%s" % (m, self.start_time, self.end_time, identifier)
            status, output = commands.getstatusoutput(cmd)
            try:
                value = round(json.loads(output)["Datapoints"][0]["Average"])
            except Exception,e:
                value = -1
            d = int(time.mktime(time.strptime(json.loads(output)["Datapoints"][0]["Timestamp"], "%Y-%m-%dT%H:%M:%SZ")))
            i = {
                'metric': '%s.%s' % (self.metric, m),
                'endpoint': '%s-%s' % (identifier, self.endpoint),
                'timestamp': d,
                'step': self.step,
                'value': value,
                'counterType': self.counterType,
                'tags': identifier
            }
            self.p.append(i)
        

    def run(self):
        """
        """
        pool = Pool(10)
        try:
            for rdsidentifier in self.rds_instance_identifier:
                pool.apply_async(self.get_rds_statistic, (rdsidentifier, ))
        except Exception,e:
            logging.error(e)
        pool.close()
        pool.join()
        r = requests.post("http://127.0.0.1:1988/v1/push", data=json.dumps(self.p))

if __name__ == "__main__":
    Resource().run()

