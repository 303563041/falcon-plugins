#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import time
import json

ts = int(time.time())
URL = "http://townkins-hub-global.funplusgame.com/account/idp/v1/api/timecheck/"
timeout = 5

try:
    rs = requests.get(URL, timeout=timeout)
    value = 1
except Exception, e:
    value = -1
d = []

i = {
    'metric': "idp.status",
    'endpoint': "Townkins-global-prod-hub",
    'timestamp': ts,
    'step': 60,
    'value': value,
    'counterType': "GAUGE",
    'tags': ""
}
d.append(i)
print json.dumps(d)
