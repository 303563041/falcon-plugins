#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import time
import json

ts = int(time.time())
URL = "http://localhost:8080/account/idp/v1/api/timecheck/"
config_path = "/data/open-falcon/cfg.json"

with open(config_path, 'r') as config:
    c = json.loads(config.read())

endpoint = c["hostname"]

timeout = 5
tags = ""
for k, v in c["default_tags"].items():
    t = k + "=" + v
    tags = tags + t + ","

try:
    rs = requests.get(URL, timeout=timeout)
    value = 1
except Exception, e:
    value = -1
d = []

i = {
    'metric': "idp.status",
    'endpoint': endpoint,
    'timestamp': ts,
    'step': 60,
    'value': value,
    'counterType': "GAUGE",
    'tags': tags.strip(",")
}
d.append(i)
print json.dumps(d)
