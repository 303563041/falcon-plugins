#!/bin/env python
#-*- coding:utf-8 -*-

import sys
import urllib2
import base64
import json
import time
import socket


step = 60
ip = socket.gethostname()
ts = int(time.time())
tag = ''
p = []


def send_request(url):
    try:
        request = urllib2.Request(url)
        # see #issue4
        base64string = base64.b64encode('monitor:tSXo1PfQAg76dHbLQu')
        request.add_header("Authorization", "Basic %s" % base64string)
        result = urllib2.urlopen(request)
        data = json.loads(result.read())
        return data
    except Exception, e:
        print "URL: %s , Send request error: %s" % (url, e)
        sys.exit(1)


def get_queue():
    keys = ('messages_ready', 'messages_unacknowledged')
    rates = ('ack', 'deliver', 'deliver_get', 'publish')
    url = "http://%s:5673/api/queues" % ip
    data = send_request(url)
    if not data:
        print "api queues no data!"
        sys.exit(1)
    for queue in data:
        # ready and unack
        msg_total = 0
        for key in keys:
            q = {}
            q["endpoint"] = ip
            q['timestamp'] = ts
            q['step'] = step
            q['counterType'] = "GAUGE"
            q['metric'] = 'rabbitmq.%s' % key
            q['tags'] = 'name=%s,%s' % (queue['name'],tag)
            q['value'] = int(queue[key])
            msg_total += q['value']
            p.append(q)

        # total
        q = {}
        q["endpoint"] = ip
        q['timestamp'] = ts
        q['step'] = step
        q['counterType'] = "GAUGE"
        q['metric'] = 'rabbitmq.messages_total'
        q['tags'] = 'name=%s,%s' % (queue['name'],tag)
        q['value'] = msg_total
        p.append(q)

        # rates
        for rate in rates:
            q = {}
            q["endpoint"] = ip
            q['timestamp'] = ts
            q['step'] = step
            q['counterType'] = "GAUGE"
            q['metric'] = 'rabbitmq.%s_rate' % rate
            q['tags'] = 'name=%s,%s' % (queue['name'],tag)
            try:
                q['value'] = int(queue['message_stats']["%s_details" % rate]['rate'])
            except:
                q['value'] = 0
            p.append(q)


def get_node_name():
    url = "http://%s:5673/api/overview" % ip
    result = send_request(url)
    if result:
        return result['node'].encode('utf-8')


def get_node_status():
    keys = ['mem_limit', 'disk_free_limit', 'mem_used', 'fd_used', 'sockets_used', 'proc_used']
    node_name = get_node_name()
    url = "http://%s:5673/api/nodes/%s" % (ip, node_name)
    result = send_request(url)
    if not result:
        print "get_node_status is error!"
        sys.exit(1)
    for key in keys:
        q = {}
        q["endpoint"] = ip
        q['timestamp'] = ts
        q['step'] = step
        q['counterType'] = "GAUGE"
        q['metric'] = 'rabbitmq.%s' % key
        q['value'] = int(result[key])
        p.append(q)


def send_falcon():
    method = "POST"
    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)
    url = 'http://127.0.0.1:1999/v1/push'
    request = urllib2.Request(url, data=json.dumps(p))
    request.add_header("Content-Type", 'application/json')
    request.get_method = lambda: method
    try:
        connection = opener.open(request)
    except urllib2.HTTPError, e:
        connection = e

    # check. Substitute with appropriate HTTP code.
    if connection.code == 200:
        print connection.read()
    else:
        print '{"err":1,"msg":"%s"}' % connection


if __name__ == "__main__":
    get_queue()
    get_node_status()
    send_falcon()
    # print json.dumps(p, indent=4)
