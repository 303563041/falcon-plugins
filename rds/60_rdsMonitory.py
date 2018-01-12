#!/usr/bin/env python
# -*- coding: utf-8 -*-

import get_rds_instances
import MySQLdb
import logging
from multiprocessing.pool import ThreadPool as Pool 
import time
import json
import sys


class Resource():

    def __init__(self):
        self.user = "readonly"
        self.passwd = "v%v7@5X8SnB8"
        self.port = 3306
        r = get_rds_instances.GetRdsInstancesList()
        self.rds_instance_endpoints = r.get_rds_endpoints()
        self.step = 60
        self.ts = int(time.time())
        self.monitor_keys = [
            ('Com_select','COUNTER'),
            ('Qcache_hits','COUNTER'),
            ('Com_insert','COUNTER'),
            ('Com_update','COUNTER'),
            ('Com_delete','COUNTER'),
            ('Com_replace','COUNTER'),
            ('MySQL_QPS','COUNTER'),
            ('MySQL_TPS','COUNTER'),
            ('ReadWrite_ratio','GAUGE'),
            ('Innodb_buffer_pool_read_requests','COUNTER'),
            ('Innodb_buffer_pool_reads','COUNTER'),
            ('Innodb_buffer_read_hit_ratio','GAUGE'),
            ('Innodb_buffer_pool_pages_flushed','COUNTER'),
            ('Innodb_buffer_pool_pages_free','GAUGE'),
            ('Innodb_buffer_pool_pages_dirty','GAUGE'),
            ('Innodb_buffer_pool_pages_data','GAUGE'),
            ('Bytes_received','COUNTER'),
            ('Bytes_sent','COUNTER'),
            ('Innodb_rows_deleted','COUNTER'),
            ('Innodb_rows_inserted','COUNTER'),
            ('Innodb_rows_read','COUNTER'),
            ('Innodb_rows_updated','COUNTER'),
            ('Innodb_os_log_fsyncs','COUNTER'),
            ('Innodb_os_log_written','COUNTER'),
            ('Created_tmp_disk_tables','COUNTER'),
            ('Created_tmp_tables','COUNTER'),
            ('Connections','COUNTER'),
            ('Innodb_log_waits','COUNTER'),
            ('Slow_queries','COUNTER'),
            ('Binlog_cache_disk_use','COUNTER')
        ]
        self.p = []

    def get_mysql_statistic(self, endpoint):
        """
        get mysql monitor data
        """
        conn = MySQLdb.connect(host=endpoint, user=self.user, passwd=self.passwd, port=self.port, charset="utf8")

        query = "SHOW GLOBAL STATUS"
        cursor = conn.cursor()
        cursor.execute(query)
        Str_string = cursor.fetchall()
        Status_dict = {}
        for Str_key,Str_value in Str_string:
            Status_dict[Str_key] = Str_value

        for _key,falcon_type in self.monitor_keys:
            if _key == 'MySQL_QPS':
                _value = int(Status_dict.get('Com_select',0)) + int(Status_dict.get('Qcache_hits',0))
            elif _key == 'MySQL_TPS':
                _value = int(Status_dict.get('Com_insert',0)) + int(Status_dict.get('Com_update',0)) + int(Status_dict.get('Com_delete',0)) + int(Status_dict.get('Com_replace',0))
            elif _key == 'Innodb_buffer_read_hit_ratio':
                try:
                    _value = round((int(Status_dict.get('Innodb_buffer_pool_read_requests',0)) - int(Status_dict.get('Innodb_buffer_pool_reads',0)))/int(Status_dict.get('Innodb_buffer_pool_read_requests',0)) * 100,3)
                except ZeroDivisionError:
                    _value = 0
            elif _key == 'ReadWrite_ratio':
                try:
                    _value = round((int(Status_dict.get('Com_select',0)) + int(Status_dict.get('Qcache_hits',0)))/(int(Status_dict.get('Com_insert',0)) + int(Status_dict.get('Com_update',0)) + int(Status_dict.get('Com_delete',0)) + int(Status_dict.get('Com_replace',0))),2)
                except ZeroDivisionError:
                    _value = 0            
            else:
                _value = int(Status_dict.get(_key,0))

            i = {
                    'metric': 'rds.%s' % (_key),
                    'endpoint': '%s-townkins-rds' % endpoint.split('.')[0],
                    'timestamp': self.ts,
                    'step': self.step,
                    'value': _value,
                    'counterType': falcon_type,
                    'tags': ""
            }

            self.p.append(i)
        cursor.close()
        conn.close()


    def run(self):
        """
        pool = Pool(10)
        try:
            for endpoint in self.rds_instance_endpoints:
                pool.apply_async(self.get_mysql_statistic, (endpoint, ))
        except Exception,e:
            logging.error(e)
        pool.close()
        pool.join()
        """
        for endpoint in self.rds_instance_endpoints:
            try:
                self.get_mysql_statistic(endpoint)
            except Exception,e:
                logging.error(e)
                continue
        print json.dumps(self.p)


if __name__ == "__main__":
    Resource().run()
