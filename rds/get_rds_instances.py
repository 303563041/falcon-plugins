#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import commands

class GetRdsInstancesList():

    def __init__(self):
        self.endpoint = "aws rds describe-db-instances  --query 'DBInstances[].Endpoint.[Address]' --output text"
        self.identifier = "aws rds describe-db-instances  --query 'DBInstances[].[DBInstanceIdentifier,StorageType]' --output text"

    def get_rds_endpoints(self):
        """
        get rds instance endpoints
        """
        status, output = commands.getstatusoutput(self.endpoint)
        rds_instance_endpoints = output.split()
        return rds_instance_endpoints

    def get_rds_identifier(self):
        """
        get rds instance identifier
        """
        status, output = commands.getstatusoutput(self.identifier)
        rds_instance_identifier = output.split('\n')
        return rds_instance_identifier
