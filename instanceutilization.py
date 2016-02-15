#!/usr/bin/env python
 
__author__ = 'facundo_nishiwaki'
 
import boto.ec2
from boto.ec2 import instance
import json
import os
 
from itertools import chain
import socket, sys
ACCOUNTS = {
    "HTCCSDEV" : {
        "ACCESS_KEY" : "DEV_ACCESS_KEY",
        "SECRET_KEY" : "DEV_SECRET_KEY"
    },
    "HTCCSCMS" : {
        "ACCESS_KEY" : "CMS_ACCESS_KEY",
        "SECRET_KEY" : "CMS_SECRET_KEY"
    },
    "HTCCSPROD" : {
        "ACCESS_KEY" : "PROD_ACCESS_KEY",
        "SECRET_KEY" : "PROD_SECRET_KEY"
    },
    "HTCCSMASTER" : {
        "ACCESS_KEY" : "MASTER_ACCESS_KEY",
        "SECRET_KEY" : "MASTER_SECRET_KEY"
    }
}
 
NORMALIZATION_FACTORS = {
    "small" : 1,
    "medium" : 2,
    "large" : 4,
    "xlarge" : 8,
    "2xlarge" : 16,
    "4xlarge" : 32,
    "8xlarge" : 64
}
 
class EC2Account:
    def __init__(self, name, access_key, secret_key):
        self.name = name
        self._access_key = access_key
        self._secret_key = secret_key
        self._instance_utilization = {}
        self._all_instance_types = []
        self._all_instances = {}
        self._all_reserved_instances = {}
        self._zones = {}
 
    def instances_in_region(self, region):
        region_info = boto.ec2.connect_to_region(region, aws_access_key_id=self._access_key, aws_secret_access_key=self._secret_key)
        self._zones[region] = region_info.get_all_zones()
        instances = region_info.get_only_instances()
        self._all_instances[region] = instances
        #all_instance_types = list(set([instance.instance_type for instance in instances]))
        #all_availability_zones = list(set([instance.placement for instance in instances]))
        #all_vpc_ids = list(set([instance.vpc_id for instance in instances]))
        return self._all_instances[region]
 
    def get_instance_counts_from_az(self, zone_name, instance_list):
        retVal = {}
        all_instance_types = list(set([i.instance_type for i in instance_list]))
        for type in all_instance_types:
            retVal[type] = len([i for i in instance_list if i.placement == zone_name and i.instance_type == type])
            if retVal[type] == 0:
                retVal.pop(type)
        return retVal
 
    def get_reserved_instance_counts(self, instance_list):
        all_availability_zones = list(set([i.availability_zone for i in instance_list]))
        retVal = {}
 
        for az in all_availability_zones:
            retVal[az] = self.get_reserved_instance_counts_from_az(az, instance_list)
        return retVal
 
    def get_reserved_instance_counts_from_az(self, zone_name, instance_list):
        all_instance_types = list(set([i.instance_type for i in instance_list]))
        ret_val = {}
        for instance_type in all_instance_types:
            ret_val[instance_type] = sum([r.instance_count for r in instance_list if
                                          r.instance_type == instance_type and
                                          r.availability_zone == zone_name])
        return ret_val
 
    def list_instances(self, region, **kwargs):
        retVal = {}
        region_info = boto.ec2.connect_to_region(region, aws_access_key_id=self._access_key, aws_secret_access_key=self._secret_key)
        self._zones[region] = region_info.get_all_zones()
        self._all_instances[region] = region_info.get_only_instances()
 
        if region not in self._all_instances.keys():
            return retVal
        if "vpc" in kwargs.keys():
            if kwargs["vpc"]:
                instances = [i for i in self._all_instances[region] if i.vpc_id is not None and i.state == "running"]
            elif not kwargs["vpc"]:
                instances = [i for i in self._all_instances[region] if i.vpc_id is None]
            kwargs.pop("vpc")
        else:
            instances = self._all_instances[region]
        args = dict([(k, kwargs[k]) for k in kwargs.keys() if hasattr(boto.ec2.instance.Instance, k) ])
 
        for zone in self._zones[region]:
            retVal[zone.name] = self.get_instance_counts_from_az(zone.name, instances)
        return retVal
 
 
    def list_reserved_instances(self, region, **kwargs):
        region_info = boto.ec2.connect_to_region(region, aws_access_key_id=self._access_key, aws_secret_access_key=self._secret_key)
 
        if "vpc" in kwargs.keys():
            if kwargs["vpc"]:
                kwargs["product_description"] = "Linux/UNIX (Amazon VPC)"
            else:
                kwargs["product_description"] = "Linux/UNIX"
            kwargs.pop("vpc")
        kwargs["state"] = "active"
        #zones = region_info.get_all_zones()
 
        reserved_instances = region_info.get_all_reserved_instances(filters=kwargs)
        self._all_reserved_instances[region] = [ri for ri in reserved_instances]
        return self.get_reserved_instance_counts(self._all_reserved_instances[region])
 
class ConsolidatedUtilization:
    def __init__(self):
        self.instance_types = []
        self.accounts_utilization = {}
        self.accounts_reserved_instance_utilization = {}
        self.total_running_instance_utilization = {}
        self.total_reserved_instance_utilization = {}
        self.actual_reserved_instance_usage = {}
        self.unused_reserved_instance_count = {}
 
    def add_account_running_instance_utilization(self, account_name, utilization):
        #self.total_running_instance_utilization[account_name] = utilization
        for region, region_utilization in utilization.iteritems():
            self.total_running_instance_utilization = \
                self.add_dictionaries(
                    self.total_running_instance_utilization,
                    region,
                    region_utilization)
            #self.add_region_running_instance_utilization(region, region_utilization)
 
    def add_account_reserved_instance_utilization(self, account_name, utilization):
        #self.accounts_reserved_instance_utilization[account_name] = utilization
        for region, region_utilization in utilization.iteritems():
            self.total_reserved_instance_utilization = \
                self.add_dictionaries(
                    self.total_reserved_instance_utilization,
                    region,
                    region_utilization)
        #for region, region_utilization in utilization.iteritems():
        #    self.add_region_reserved_instance_utilization(region, region_utilization)
 
    def get_reserved_instance_usage(self, region):
        self.actual_reserved_instance_usage[region] = self.subtract_dictionaries(
            self.total_reserved_instance_utilization[region],
            self.total_running_instance_utilization[region])
        return self.actual_reserved_instance_usage[region]
 
    def get_unused_reserved_instance_count(self, region):
        actual_usage = self.get_reserved_instance_usage(region)
        self.unused_reserved_instance_count[region] = {}
        for zone in actual_usage.keys():
            self.unused_reserved_instance_count[region][zone] = dict([(instance_type, -1* count)
                                                            if count > 0 else (instance_type, 0)
                                                            for (instance_type, count) in actual_usage[zone].iteritems()])
        return self.unused_reserved_instance_count[region]
 
    def get_instance_class_unit_usage(self, region):
        ret_val = {}
        reserved_instance_usage = self.get_reserved_instance_usage(region)
        instance_types = list(set([reserved_instance_usage[zone].keys() for zone in reserved_instance_usage.keys()]))
        classes = list(set([instance_type.split(".")[0] for instance_type in instance_types]))
        for c in classes:
            ret_val[c] = sum([self.get_instance_type_unit_count(region, instance_type) for instance_type in instance_types if instance_type.startswith(c)])
        return ret_val
 
    def get_instance_type_unit_count(self, region, instance_type):
        class_type, class_size = instance_type.split(".")
        counts = [self.get_reserved_instance_usage(region)[zone][instance_type] for zone in self.get_reserved_instance_usage(region).keys()]
 
        return NORMALIZATION_FACTORS[class_size] * sum(counts)
 
    def add_dictionaries(self, parent_dict, node_name, child):
        ret_val = parent_dict
        if node_name in parent_dict.keys():
            if type(parent_dict[node_name]).__name__ == "dict":
                for sub_node_name, grandchild in child.iteritems():
                    self.add_dictionaries(ret_val[node_name], sub_node_name, grandchild)
            else:
                ret_val[node_name] += child
        else:
            ret_val[node_name] = child
        return ret_val
 
    def subtract_dictionaries(self, d1, d2):
        ret_val = {}
 
        all_keys = list(set(d1.keys() + d2.keys()))
 
        for key in all_keys:
            if key in d1.keys():
                if type(d1[key]).__name__ == "dict":
                    if key in d2.keys():
                        ret_val[key] = self.subtract_dictionaries(d1[key], d2[key])
                    else:
                        ret_val[key] = d1[key]
                else:
                    if key in d2.keys():
                        ret_val[key] = d1[key] - d2[key]
                    else:
                        ret_val[key] = d1[key]
            else:
                if type(d2[key]).__name__ == "dict":
                    ret_val[key] = self.subtract_dictionaries({}, d2[key])
                else:
                    ret_val[key] = 0 - d2[key]
 
        return ret_val
 
    def get_reserved_instance_utilization_totals(self, region):
        return self.total_reserved_instance_utilization[region]
 
    def get_running_instance_utilization_totals(self, region):
        return self.total_running_instance_utilization[region]
 
 
if __name__ == "__main__":
    region = "ap-northeast-1"
    zone   = "ap-northeast-1a"
    instance_type = "m2.2xlarge"
    running_instances = {}
    reserved_instances = {}
    totals = ConsolidatedUtilization()
    actual = {}
    for name, account in ACCOUNTS.iteritems():
        running_instances[name] = {}
        reserved_instances[name] = {}
        account = EC2Account(name, account["ACCESS_KEY"], account["SECRET_KEY"])
 
        for region in [r.name for r in boto.ec2.regions() if r.name.find("gov") == -1 and r.name.find("cn") == -1]:
            running_instances[name][region] = account.list_instances(region, vpc=True)
            reserved_instances[name][region] = account.list_reserved_instances(region, vpc=True)
 
        totals.add_account_reserved_instance_utilization(name, reserved_instances[name])
        totals.add_account_running_instance_utilization(name, running_instances[name])
 
    #for region in [r.name for r in boto.ec2.regions() if r.name.find("gov") == -1]:
    #    print "reserved", totals.get_reserved_instance_utilization_totals(region)
    #    print "running", totals.get_running_instance_utilization_totals(region)
    for region in [r.name for r in boto.ec2.regions() if r.name.find("gov") == -1]:
        actual[region] = totals.get_reserved_instance_usage(region)
        print json.dumps(totals.get_unused_reserved_instance_count(region))
 
    print json.dumps(actual)
