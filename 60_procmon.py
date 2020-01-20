#!/usr/bin/env python
# -*- coding:utf-8 -*-

import time
import json
import re
import os
import psutil
import socket
import requests

class ProcmonCollector(object):
    def __init__(self):
        self.processes_dict = {}
        self.cpu_num = self.get_cpu_num()
        self.tmp_file_path = os.path.dirname(os.path.abspath(
            __file__)) + "/data/"
        self.endpoint = socket.gethostname()
        self.procs = ['agms', 'agtw']
        self.now_t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        self._analyze_processes_dic()

    def run(self):
        data = []
        for proc_name, configs in self.processes_dict.iteritems():
            info = self.collect_single_module(proc_name, configs)
            if info:
                data = data + info
        
        # print json.dumps(data)
        url = "http://127.0.0.1:1999/v1/push"
        try:
            requests.post(url, data=json.dumps(data))
        except Exception as e:
            print("[ {0} ] Send data to falcon error, ".format(self.now_t), e)

    def collect_single_module(self, proc_name, configs):
        """
        Generate merged process detail info
        :param tags: (str) the key of self.processes_dict, which will be used to generate tags of the final records.
        :param configs: (dict) including collect configs(from json file), process instances etc.
        :return: info(ProcessesMergedInfo) or None if fails
        """
        try:
            info = ProcessesMergedInfo()
            info.module = proc_name
            info.ts = int(time.time())
            info.endpoint = self.endpoint

            # merge all processes' collected data
            for p_ins in configs.get('processes', []):
                # fd_usage
                num_fds, fd_limit = self.collect_fd_usage(p_ins)
                info.fd_total += num_fds
                info.fd_usage = max(info.fd_usage, float(
                    num_fds) / fd_limit) if fd_limit > 0 else info.fd_usage
                # cpu_usage
                cpu_user, cpu_sys = self.collect_cpu_usage(p_ins)
                info.cpu_usr_ticks += cpu_user
                info.cpu_sys_ticks += cpu_sys
                # io_usage (k/s)
                read_bytes, write_bytes = self.collect_io_usage(p_ins)
                info.io_read_kb += float(read_bytes) / 1024
                info.io_write_kb += float(write_bytes) / 1024
                # process status and thread num
                num_threads = self.collect_process_threads(p_ins)
                info.thread_num += num_threads
                info.process_num += 1
                # mem_usage (m/s)
                mem_vms, mem_rss, mem_shared = self.collect_memory_usage(p_ins)
                info.memory_res += float(mem_rss) / 1024 / 1024

            new_records = {
                'probe_timestamp': info.ts,
                'cpu_sys_ticks': info.cpu_sys_ticks,
                'cpu_user_ticks': info.cpu_usr_ticks,
                'io_read_kb': info.io_read_kb,
                'io_write_kb': info.io_write_kb
            }

            # load saved ts, io, cpu info
            tmp_file_name = configs['tmp_file_prefix']

            if not os.path.exists(self.tmp_file_path):
                os.makedirs(self.tmp_file_path)

            if os.path.exists(tmp_file_name):
                with open(tmp_file_name, 'r') as fp_read:
                    old_records = json.load(fp=fp_read)
            else:
                old_records = new_records

            # save(update) ts, io, cpu info
            with open(tmp_file_name, 'w') as fp_write:
                json.dump(fp=fp_write, obj=new_records)

            # calc rates
            time_gap = max(
                float(new_records['probe_timestamp'] - old_records['probe_timestamp']), 1.0)
            info.cpu_usage_total = (new_records['cpu_sys_ticks'] - old_records['cpu_sys_ticks'] +
                                    new_records['cpu_user_ticks'] - old_records['cpu_user_ticks']) / time_gap
            info.cpu_usage_average = info.cpu_usage_total / self.cpu_num
            info.io_write_rate = (
                new_records['io_write_kb'] - old_records['io_write_kb']) / time_gap
            info.io_read_rate = (
                new_records['io_read_kb'] - old_records['io_read_kb']) / time_gap

            return info.rs()
        except Exception, e:
            return None


    def _analyze_processes_dic(self):
        try:
            for p in self.procs:
                self.processes_dict.setdefault(p, {})
                self.processes_dict[p]['tmp_file_prefix'] = self.tmp_file_path + p

            for p_ins in psutil.process_iter():
                process_name = p_ins.name()
                for k, v in self.processes_dict.iteritems():
                    self.processes_dict[k].setdefault('processes', [])
                    if k == process_name:
                        self.processes_dict[k]['processes'].append(p_ins)
        except Exception, e:
            pass

    @staticmethod
    def get_cpu_num():
        """
        Get cpu num of OS.
        :return: the cpu num or None if failed
        """
        try:
            return psutil.cpu_count()
        except Exception, e:
            return None

    @staticmethod
    def collect_fd_usage(p_ins):
        """
        Collect the fd usage and limit.
        :param p_ins: (Process ins) An instance of psutil.Process
        :return: fd_usage and fd_limit or 0 if collection fails
        """
        try:
            num_fds = p_ins.num_fds()
            limits_file = "/proc/{pid}/limits".format(pid=p_ins.pid)

            with open(limits_file, 'r') as fp:
                # find the limit using regex
                patten = r'^Max open files\s+(\d+)\s+(\d+)\s+.+$'
                for line in fp:
                    res = re.match(pattern=patten, string=line)
                    if res:
                        soft_limit, hard_limit = map(int, res.groups())
                        break
                fd_limit = soft_limit if soft_limit >= hard_limit else hard_limit
            return num_fds, fd_limit

        except Exception, e:
            return 0, 0

    @staticmethod
    def collect_cpu_usage(p_ins):
        """
        Collect the cpu usage(both user and sys).
        :param p_ins: (Process ins) An instance of psutil.Process
        :return: cpu_usr and cpu_sys or 0 if collection fails
        """
        try:
            stat_file = "/proc/{pid}/stat".format(pid=p_ins.pid)
            with open(stat_file, 'r') as fp:
                content_list = re.split(pattern=r"\s+", string=fp.read())
            return map(int, content_list[13:15])

        except Exception, e:
            return 0, 0

    @staticmethod
    def collect_memory_usage(p_ins):
        """
        Collect the memory usage.
        :param p_ins: (Process ins) An instance of psutil.Process
        :return: virtual, resident and shared memory or 0 if collection fails
        """
        try:
            mem_usage = p_ins.memory_info()
            mem_rss = mem_usage.rss
            mem_vms = mem_usage.vms
            mem_shared = mem_usage.shared
            return mem_vms, mem_rss, mem_shared

        except Exception, e:
            return 0, 0, 0

    @staticmethod
    def collect_io_usage(p_ins):
        """
        Collect the io usage.
        :param p_ins: (Process ins) An instance of psutil.Process
        :return: read, write(in bytes) or 0 if collection fails
        """
        try:
            io_usage = p_ins.io_counters()
            read_bytes = io_usage.read_bytes
            write_bytes = io_usage.write_bytes
            return read_bytes, write_bytes

        except Exception, e:
            return 0, 0

    @staticmethod
    def collect_process_threads(p_ins):
        """
        Collect the num of threads in a process.
        :param p_ins: (Process ins) An instance of psutil.Process
        :return: num_threads or 0 if collection fails
        """
        try:
            num_threads = p_ins.num_threads()
            return num_threads

        except Exception, e:
            return 0


class ProcessesMergedInfo(object):
    def __init__(self):
        self.module = ""
        self.ts = 0
        self.endpoint = ""
        self.tags = "env=qa"

        self.fd_total = 0
        self.fd_usage = 0.0
        self.cpu_usr_ticks = 0
        self.cpu_sys_ticks = 0
        self.cpu_usage_total = 0.0
        self.cpu_usage_average = 0.0
        self.io_write_kb = 0
        self.io_read_kb = 0
        self.io_write_rate = 0
        self.io_read_rate = 0
        self.process_num = 0
        self.thread_num = 0
        self.memory_res = 0

    def rs(self):
        """
        Standard format of collector-fluentd
        :return:
        """
        try:
            record_list = list()
            record_list.append(self._format_record(
                self.module, self.endpoint, "process.num", self.ts, self.process_num, self.tags))
            record_list.append(self._format_record(
                self.module, self.endpoint, "thread.num", self.ts, self.thread_num, self.tags))

            if self.process_num > 0:
                record_list.append(self._format_record(
                    self.module, self.endpoint, "memory.res", self.ts, self.memory_res, self.tags))
                record_list.append(self._format_record(
                    self.module, self.endpoint, "fd.total", self.ts, self.fd_total, self.tags))
                record_list.append(self._format_record(
                    self.module, self.endpoint, "fd.usage", self.ts, self.fd_usage, self.tags))
                record_list.append(self._format_record(
                    self.module, self.endpoint, "cpu.usage.total", self.ts, self.cpu_usage_total, self.tags))
                record_list.append(self._format_record(
                    self.module, self.endpoint, "cpu.usage.average", self.ts, self.cpu_usage_average, self.tags))
                record_list.append(self._format_record(
                    self.module, self.endpoint, "io.read", self.ts, self.io_read_rate, self.tags))
                record_list.append(self._format_record(
                    self.module, self.endpoint, "io.write", self.ts, self.io_write_rate, self.tags))

            return record_list
        except Exception, e:
            return ""

    @staticmethod
    def _format_record(monitor_module, endpoint, key, ts, val, tags):
        return {
            'metric': key,
            'endpoint': endpoint,
            'timestamp': ts,
            'step': 60,
            'value': val,
            'counterType': "GAUGE",
            'tags': tags + ",name={0}".format(monitor_module)
        }


if __name__ == "__main__":
    ins = ProcmonCollector()
    ins.run()
