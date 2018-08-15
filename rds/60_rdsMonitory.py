#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import logging
from multiprocessing.pool import ThreadPool as Pool
import time
import json
import sys
import boto3


class Resource():

    def __init__(self):
        self.user = "readonly"
        self.passwd = "v%v7@5X8SnB8"
        self.port = 3306
        self.rdsEndpoints = []
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
            ('Binlog_cache_disk_use','COUNTER'),
            ('Status', 'GAUGE')
        ]
        self.p = []

    def getRdsEndpoints(self):
        """
        get rds identifier list to self.rdsIdentifiers parameters
        """
        client = boto3.client('rds')
        response = client.describe_db_instances()
        for r in response["DBInstances"]:
            if r["DBInstanceStatus"] == "available":
                if "sandbox" in r["DBInstanceIdentifier"] or "staging" in r["DBInstanceIdentifier"]:
                    continue
                endpoint = r["Endpoint"]["Address"]
                self.rdsEndpoints.append(endpoint)

    def get_mysql_statistic(self, endpoint):
        """
        Undo_Log_Length     GAUGE  未清除的Undo事务数
        Com_select  COUNTER     select/秒=QPS
        Com_insert  COUNTER     insert/秒
        Com_update  COUNTER     update/秒
        Com_delete  COUNTER     delete/秒
        Com_replace     COUNTER     replace/秒
        MySQL_QPS   COUNTER     QPS
        MySQL_TPS   COUNTER     TPS 
        ReadWrite_ratio     GAUGE   读写比例
        Innodb_buffer_pool_read_requests    COUNTER     innodb buffer pool 读次数/秒
        Innodb_buffer_pool_reads    COUNTER     Disk 读次数/秒
        Innodb_buffer_read_hit_ratio    GAUGE   innodb buffer pool 命中率
        Innodb_buffer_pool_pages_flushed    COUNTER     innodb buffer pool 刷写到磁盘的页数/秒
        Innodb_buffer_pool_pages_free   GAUGE   innodb buffer pool 空闲页的数量
        Innodb_buffer_pool_pages_dirty  GAUGE   innodb buffer pool 脏页的数量
        Innodb_buffer_pool_pages_data   GAUGE   innodb buffer pool 数据页的数量
        Bytes_received  COUNTER     接收字节数/秒
        Bytes_sent  COUNTER     发送字节数/秒
        Innodb_rows_deleted     COUNTER     innodb表删除的行数/秒
        Innodb_rows_inserted    COUNTER     innodb表插入的行数/秒
        Innodb_rows_read    COUNTER     innodb表读取的行数/秒
        Innodb_rows_updated     COUNTER     innodb表更新的行数/秒
        Innodb_os_log_fsyncs    COUNTER     Redo Log fsync次数/秒 
        Innodb_os_log_written   COUNTER     Redo Log 写入的字节数/秒
        Created_tmp_disk_tables     COUNTER     创建磁盘临时表的数量/秒
        Created_tmp_tables  COUNTER     创建内存临时表的数量/秒
        Connections     COUNTER     连接数/秒
        Innodb_log_waits    COUNTER     innodb log buffer不足等待的数量/秒
        Slow_queries    COUNTER     慢查询数/秒
        Binlog_cache_disk_use   COUNTER     Binlog Cache不足的数量/秒
        """
        try:
            conn = MySQLdb.connect(
                host=endpoint,
                user=self.user,
                passwd=self.passwd,
                port=self.port,
                charset="utf8",
                connect_timeout=10)
        except Exception, e:
            i = {
                'metric': 'mysql.mStatus',
                'endpoint': 'Townkins-rds-%s' % endpoint.split('.')[0],
                'timestamp': self.ts,
                'step': self.step,
                'value': -1,
                'counterType': 'GAUGE',
                'tags': ""
            }
            self.p.append(i)
        cursor = conn.cursor()
        query = "SHOW GLOBAL STATUS"
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
                    _value = -1
            elif _key == 'ReadWrite_ratio':
                try:
                    _value = round((int(Status_dict.get('Com_select',0)) + int(Status_dict.get('Qcache_hits',0)))/(int(Status_dict.get('Com_insert',0)) + int(Status_dict.get('Com_update',0)) + int(Status_dict.get('Com_delete',0)) + int(Status_dict.get('Com_replace',0))),2)
                except ZeroDivisionError:
                    _value = -1
            elif _key == 'Status':
                _value = 1
            else:
                _value = int(Status_dict.get(_key,-1))

            i = {
                    'metric': 'mysql.m%s' % (_key),
                    'endpoint': 'Townkins-rds-%s' % endpoint.split('.')[0],
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
        main
        """
        self.getRdsEndpoints()
        pool = Pool(10)
        for endpoint in self.rdsEndpoints:
            try:
                pool.apply_async(self.get_mysql_statistic, (endpoint, ))
            except Exception,e:
                logging.error(e)
                continue
        pool.close()
        pool.join()

        print json.dumps(self.p)


if __name__ == "__main__":
    Resource().run()
