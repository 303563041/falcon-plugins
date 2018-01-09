#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import urllib2
import base64
import json
import time
import socket
import logging


class Resource():

    def __init__(self):
        self.config = '/data/open-falcon/cfg.json'
        with open(self.config) as cfg:
            self.data = json.load(cfg)
        self.host = self.data['hostname']
        self.step = 60
        self.counterType = "GAUGE"
        self.ts = int(time.time())
        self.keys = ('messages_ready', 'messages_unacknowledged')
        self.rates = ('ack', 'deliver', 'deliver_get', 'publish')
        self.logger = logging.getLogger("Rabbitmq")
        self.logger.setLevel(logging.INFO)
        self.tag = ''

    def connMq(self):
        try:
            request = urllib2.Request("http://localhost:15672/api/queues")
            base64string = base64.b64encode("admin:admin")
            request.add_header("Authorization", "Basic %s" % base64string)
            result = urllib2.urlopen(request)
            data = json.loads(result.read())
        except Exception,e:
            self.logger.error(e)
            sys.exit()
        return data

    def get_data(self):
        '''
        rabbitmq.messages_ready: 队列中处于等待被消费状态消息数 
        rabbitmq.messages_unacknowledged: 队列中处于消费中状态的消息数
        rabbitmq.messages_total: 队列中所有未完成消费的消息数，等于messages_ready+messages_unacknowledged
        rabbitmq.ack_rate: 消费者ack的速率
        rabbitmq.deliver_rate: deliver的速率
        rabbitmq.deliver_get_rate: deliver_get的速率
        rabbitmq.publish_rate: publish的速率
        '''
        p = []
        for queue in self.connMq():
            # ready and unack
            msg_total = 0
            for key in self.keys:
                q = {}
                q["endpoint"] = self.host
                q["timestamp"] = self.ts
                q["step"] = self.step
                q["counterType"] = self.counterType
                q["metric"] = "rabbitmq.%s" % key
                q["value"] = int(queue[key])
                q["tags"] = "vhost=%s,name=%s" % (queue["vhost"], queue["name"])
                msg_total += q["value"]
                p.append(q)

            # total
            q = {}
            q["endpoint"] = self.host
            q["timestamp"] = self.ts
            q["step"] = self.step
            q["counterType"] = self.counterType
            q["metric"] = "rabbitmq.messages_total"
            q["tags"] = "vhost=%s,name=%s" % (queue["vhost"], queue["name"])
            q["value"] = msg_total
            p.append(q)

            # rates
            for rate in self.rates:
                q = {}
                q["endpoint"] = self.host
                q["timestamp"] = self.ts
                q["step"] = self.step
                q["counterType"] = self.counterType
                q["metric"] = "rabbitmq.%s_rate" % rate
                q["tags"] = "vhost=%s,name=%s" % (queue["vhost"], queue["name"])
                try:
                    q["value"] = int(queue["message_stats"]["%s_details" % rate]["rate"])
                except Exception:
                    q["value"] = 0
                p.append(q)
        return p

    def run(self):
        return self.get_data()

if __name__ == "__main__":
    d = Resource().run()
    if d:
        print json.dumps(d)
