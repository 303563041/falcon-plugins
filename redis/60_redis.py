#!/usr/bin/env python
#-*- coding: utf-8 -*-

import redis
import json
import time
import logging
import sys

class Resource():

    def __init__(self):
        self.config = '/data/open-falcon/cfg.json'
        with open(self.config) as cfg:
            self.data = json.load(cfg)
        self.endpoint = self.data['hostname']
        self.step = 60
        self.ts = int(time.time())
        self.host = "localhost"
        self.port = 6379
        self.logger = logging.getLogger("Redis")
        self.logger.setLevel(logging.INFO)
        self.metric = "redis"
        self.tags = "port=%s" % self.port

    def redis_conn(self):
        """
        redis connection
        """
        try:
            r = redis.Redis(host=self.host, port=self.port)
        except Exception,e:
            self.logger.error(e)
            sys.exit()
        info = r.info()
        return info

    def get_data(self):
        """
        redis.connected_clients: 已连接客户端的数量 
        redis.blocked_clients: 正在等待阻塞命令（BLPOP、BRPOP、BRPOPLPUSH）的客户端的数量
        redis.used_memory: 由 Redis 分配器分配的内存总量，以字节（byte）为单位
        redis.used_memory_rss: 从操作系统的角度，返回 Redis 已分配的内存总量（俗称常驻集大小）
        redis.mem_fragmentation_ratio: used_memory_rss 和 used_memory 之间的比率
        redis.total_commands_processed: 采集周期内执行命令总数
        redis.rejected_connections: 采集周期内拒绝连接总数
        redis.expired_keys: 采集周期内过期key总数
        redis.evicted_keys: 采集周期内踢出key总数
        redis.keyspace_hits: 采集周期内key命中总数
        redis.keyspace_misses: 采集周期内key拒绝总数
        redis.keyspace_hit_ratio: 访问命中率
        """
        info = self.redis_conn()
        monit_keys = [
            ('connected_clients','GAUGE'), 
            ('blocked_clients','GAUGE'), 
            ('used_memory','GAUGE'),
            ('used_memory_rss','GAUGE'),
            ('mem_fragmentation_ratio','GAUGE'),
            ('total_commands_processed','COUNTER'),
            ('rejected_connections','COUNTER'),
            ('expired_keys','COUNTER'),
            ('evicted_keys','COUNTER'),
            ('keyspace_hits','COUNTER'),
            ('keyspace_misses','COUNTER'),
            ('keyspace_hit_ratio','GAUGE'),
        ]

        p = []
        for key, ctype in monit_keys:
            # 计算命中率
            if key == "keyspace_hit_ratio":
                try:
                    value = float(info['keyspace_hits'])/(int(info['keyspace_hits']) + int(info['keyspace_misses']))
                except Exception,e:
                    value = 0      
            # 计算碎片率(mem_fragmentation_ratio)
            elif key == "mem_fragmentation_ratio":
                value = float(info[key])
            # 其他的都采集成counter, int
            else:
                try:
                    value = int(info[key])
                except Exception,e:
                    continue

            i = {
                'metric': '%s.%s' % (self.metric, key),
                'endpoint': self.endpoint,
                'timestamp': self.ts,
                'step': self.step,
                'value': value,
                'counterType': ctype,
                'tags': self.tags
            }
            p.append(i)
        return p

    def run(self):
        return self.get_data()

if __name__ == "__main__":
    d = Resource().run()
    if d:
        print json.dumps(d)

