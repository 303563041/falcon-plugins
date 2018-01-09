#!/bin/env python
#-*- coding:utf-8 -*-

import os,sys
import os.path
from os.path import isfile
import socket
import time
import json
import copy

class Resource():
    def __init__(self, pid):
        self.pid = pid
        self.config = '/data/open-falcon/cfg.json'
        with open(self.config) as cfg:
            self.data = json.load(cfg)
        self.host = self.data['hostname']

    def get_cpu_user(self):
        cmd="cat /proc/" + str(self.pid)  +  "/stat |awk '{print $14+$16}'"
        return os.popen(cmd).read().strip("\n")

    def get_cpu_sys(self):
        cmd="cat /proc/" + str(self.pid)  +  "/stat |awk '{print $15+$17}'"
        return os.popen(cmd).read().strip("\n")

    def get_cpu_all(self):
        cmd="cat /proc/" + str(self.pid)  +  "/stat |awk '{print $14+$15+$16+$17}'"
        return os.popen(cmd).read().strip("\n")

    def get_mem(self):
        cmd="cat /proc/" + str(self.pid)  +  "/status |grep VmRSS |awk '{print $2*1024}'"
        return os.popen(cmd).read().strip("\n")

    def get_swap(self):
        cmd="cat /proc/" + str(self.pid)  +  "/stat |awk '{print $(NF-7)+$(NF-8)}' "
        return os.popen(cmd).read().strip("\n")

    def get_fd(self):
        cmd="cat /proc/" + str(self.pid)  +  "/status |grep FDSize |awk '{print $2}'"
        return os.popen(cmd).read().strip("\n")

    def run(self):
        self.resources_d={
            'java.cpu.user':[self.get_cpu_user,'COUNTER'],
            'java.cpu.sys':[self.get_cpu_sys,'COUNTER'],
            'java.cpu.all':[self.get_cpu_all,'COUNTER'],
            'java.mem':[self.get_mem,'GAUGE'],
            'java.swap':[self.get_swap,'GAUGE'],
            'java.fd':[self.get_fd,'GAUGE']
        }

        if not os.path.isdir("/proc/" + str(self.pid)):
            return

        output = []
        for resource in  self.resources_d.keys():
                t = {}
                t['endpoint'] = self.host
                t['timestamp'] = int(time.time())
                t['step'] = 60
                t['counterType'] = self.resources_d[resource][1]
                t['metric'] = resource
                t['value']= self.resources_d[resource][0]()
                t['tags'] = 'pid=%s' %self.pid

                output.append(t)

        return output

    def dump_data(self):
        return json.dumps()

if __name__ == "__main__":
    import psutil
    pids = psutil.pids()
    java_pid = ''
    for pid in pids:
        p = psutil.Process(pid)
        name = p.name()
        if name == 'java':
            java_pid = pid
    d = Resource(java_pid).run()
    if d:
        print json.dumps(d)
