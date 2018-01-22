#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json
import sys
import logging
from multiprocessing.pool import ThreadPool as Pool
import time

class Resource():

    def __init__(self):
        self.step = 60
        self.counterType = "GAUGE"
        self.ts = int(time.time())
        self.start_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 180))
        self.end_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 120))
        self.metric_list = [
            ("HTTPCode_Backend_4XX", "Sum"), 
            ("HTTPCode_Backend_5XX", "Sum"), 
            ("HTTPCode_ELB_5XX", "Sum"), 
            ("HTTPCode_ELB_4XX", "Sum"),
            ("Latency", "Average"),
            ("UnHealthyHostCount", "Average")
        ]
        self.cloudwatch_client = boto3.client("cloudwatch")
        self.LoadBalancerName = []
        self.p = []

    def get_elb_name(self):
        """
        get LoadBalancerName
        """
        client = boto3.client("elb")
        rs = client.describe_load_balancers()["LoadBalancerDescriptions"]
        if rs:
            for r in rs:
                self.LoadBalancerName.append(r["LoadBalancerName"])
        else:
            logging.error("LoadBalancerName is Null!!!")
            sys.exit(1)

    def get_data(self, ElbName, metric, mtype):
        """
        get elb monitor data
        """
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                    Namespace = "AWS/ELB",
                    MetricName = "{0}".format(metric),
                    StartTime = "{0}".format(self.start_time),
                    EndTime = "{0}".format(self.end_time),
                    Period = 60,
                    Statistics = ["{0}".format(mtype)]
                )
            if response["Datapoints"]:
                value = response["Datapoints"][0][mtype]
            else:
                value = 0
        except Exception, e:
            print e
            value = -1

        i = {
            'metric': 'elb.{0}'.format(metric),
            'endpoint': 'elb.{0}'.format(ElbName),
            'timestamp': self.ts,
            'step': self.step,
            'value': value,
            'counterType': self.counterType,
            'tags': ""
        }
        self.p.append(i)


    def run(self):
        """
        main
        """
        self.get_elb_name()
        pool = Pool(10)
        for ElbName in self.LoadBalancerName:
            for metric, mtype in self.metric_list:
                try:
                    pool.apply_async(self.get_data, (ElbName, metric, mtype))
                except Exception,e:
                    logging.error(e)
                    continue
        pool.close()
        pool.join()

        print json.dumps(self.p)

if __name__ == "__main__":
    Resource().run()
