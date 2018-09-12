#!/bin/env python
#-*- coding:utf-8 -*-

import os, sys
import os.path
from os.path import isfile
import socket
import time
import json
import copy
import psutil


class Resource():
    def __init__(self):
        self.config = '/data/open-falcon/cfg.json'
        with open(self.config) as cfg:
            self.data = json.load(cfg)
        self.host = self.data['hostname']
        self.rs = []
        self.tags = ""
        for k, v in self.data["default_tags"].items():
            t = k + "=" + v
            self.tags = self.tags + t + ","

    def get_cpu_user(self, pid):
        cmd = "cat /proc/" + str(pid) + "/stat |awk '{print $14+$16}'"
        try:
            value = os.popen(cmd).read().strip("\n")
        except Exception, e:
            value = -1
        return value

    def get_cpu_sys(self, pid):
        cmd = "cat /proc/" + str(pid) + "/stat |awk '{print $15+$17}'"
        try:
            value = os.popen(cmd).read().strip("\n")
        except Exception, e:
            value = -1
        return value

    def get_cpu_all(self, pid):
        cmd = "cat /proc/" + str(pid) + "/stat |awk '{print $14+$15+$16+$17}'"
        try:
            value = os.popen(cmd).read().strip("\n")
        except Exception, e:
            value = -1
        return value

    def get_mem(self, pid):
        cmd = "cat /proc/" + str(
            pid) + "/status |grep VmRSS |awk '{print $2*1024}'"
        try:
            value = os.popen(cmd).read().strip("\n")
        except Exception, e:
            value = -1
        return value

    def get_swap(self, pid):
        cmd = "cat /proc/" + str(pid) + "/stat |awk '{print $(NF-7)+$(NF-8)}' "
        try:
            value = os.popen(cmd).read().strip("\n")
        except Exception, e:
            value = -1
        return value

    def get_fd(self, pid):
        cmd = "cat /proc/" + str(pid) + "/status |grep FDSize |awk '{print $2}'"
        try:
            value = os.popen(cmd).read().strip("\n")
        except Exception, e:
            value = -1
        return value

    def run(self):
        pids = psutil.pids()
        for pid in pids:
            p = psutil.Process(pid)
            try:
                if p.name() == 'java':
                    name = p.username()
                    self.resources_d = {
                        'java.cpu.user': [self.get_cpu_user, 'COUNTER'],
                        'java.cpu.sys': [self.get_cpu_sys, 'COUNTER'],
                        'java.cpu.all': [self.get_cpu_all, 'COUNTER'],
                        'java.mem': [self.get_mem, 'GAUGE'],
                        'java.swap': [self.get_swap, 'GAUGE'],
                        'java.fd': [self.get_fd, 'GAUGE']
                    }

                    if not os.path.isdir("/proc/" + str(pid)):
                        return

                    for resource in self.resources_d.keys():
                        t = {}
                        t['endpoint'] = self.host
                        t['timestamp'] = int(time.time())
                        t['step'] = 60
                        t['counterType'] = self.resources_d[resource][1]
                        t['metric'] = resource
                        t['value'] = self.resources_d[resource][0](pid)
                        t['tags'] = 'name=' + name + "," + self.tags.strip(",")
                        self.rs.append(t)
            except Exception:
                continue

        print json.dumps(self.rs)


if __name__ == "__main__":
    Resource().run()
