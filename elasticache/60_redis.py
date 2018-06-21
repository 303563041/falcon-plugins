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
        self.redisIdentifiers = []
        self.ts = int(time.time())
        self.start_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 180))
        self.end_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 120))
        self.p = []
        self.step = 60
        self.counterType = "GAUGE"
        self.redis_metric_list = [
            "CPUUtilization",
            "EngineCPUUtilization",
            "CacheHits",
            "FreeableMemory",
            "CurrConnections",
            "ReplicationLag",
            "CacheMisses",
            "NewConnections",
            "GetTypeCmds",
            "SetTypeCmds",
            "KeyBasedCmds",
            "NetworkBytesIn",
            "NetworkBytesOut",
            "HashBasedCmds"
        ]
        self.memory_mapping = {
            'cache.t2.micro': 1,
            'cache.t2.small': 2,
            'cache.t2.medium': 4,
            'cache.r3.large': 15,
            'cache.r3.xlarge': 30.5,
            'cache.r3.2xlarge': 61,
            'cache.r3.4xlarge': 122,
            'cache.r3.8xlarge': 244,
            'cache.m3.medium': 3.75,
            'cache.m3.large': 7.5,
            'cache.m3.xlarge': 15,
            'cache.m3.2xlarge': 30,
            'cache.m4.large': 8,
            'cache.m4.xlarge': 16,
            'cache.m4.2xlarge': 32,
            'cache.m4.4xlarge': 64,
            'cache.m4.10xlarge': 160,
            'cache.r4.xlarge': 32,
            'cache.r4.large': 16,
            'cache.r4.2xlarge': 64,
            'cache.r4.4xlarge': 128,
            'other': 0
        }

        self.p = []

    def getRedisIdentifiers(self):
        """
        get cacheclusterid identifier list to self.redisIdentifiers parameters
        """
        client = boto3.client('elasticache')
        response = client.describe_cache_clusters()
        for r in response["CacheClusters"]:
            if r["CacheClusterStatus"] == "available" and r["Engine"] == "redis":
                CacheClusterId = r["CacheClusterId"]
                CacheNodeType = r["CacheNodeType"]
                self.redisIdentifiers.append((CacheClusterId, CacheNodeType))

    def getRedisMonitor(self, CacheClusterId, metric, CacheNodeType):
        """
        get per redis metric statistics
        """
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                    Namespace = 'AWS/ElastiCache',
                    MetricName = '{0}'.format(metric),
                    Dimensions = [
                        {
                            'Name': 'CacheClusterId',
                            'Value': '{0}'.format(CacheClusterId)
                        },
                    ],
                    StartTime = self.start_time,
                    EndTime = self.end_time,
                    Period = 60,
                    Statistics = ['Average']
                )
            if metric == 'FreeableMemory':
                memory_total = self.memory_mapping[CacheNodeType]
                value = response["Datapoints"][0]["Average"]/(memory_total * 1024 * 1024 * 1024) * 100
            else:
                value = response["Datapoints"][0]["Average"]
        except Exception, e:
            value = -1

        i = {
            'metric': 'redis.r{0}'.format(metric),
            'endpoint': 'Townkins-redis-{0}'.format(CacheClusterId.replace('-', '_')),
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
        # get redis identifiers list
        self.getRedisIdentifiers()

        # create thread pool
        pool = Pool(10)

        # main task
        for CacheClusterId, CacheNodeType in self.redisIdentifiers:
            for metric in self.redis_metric_list:
                try:
                    pool.apply_async(self.getRedisMonitor, (CacheClusterId, metric, CacheNodeType))
                except Exception,e:
                    logging.error(e)
                    continue
        pool.close()
        pool.join()

        print json.dumps(self.p)

if __name__ == "__main__":
    Resource().run()
