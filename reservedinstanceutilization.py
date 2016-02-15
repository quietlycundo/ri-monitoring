#!/usr/bin/env python
 
__author__ = 'facundo_nishiwaki'
 
import boto.ec2
from boto.ec2 import instance
import json, csv
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
 
ACCOUNTS1 = {
    "HTCCSDEV" : {
        "ACCESS_KEY" : "AKIAIN6IAUB56BROLAEQ",
        "SECRET_KEY" : "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"
    },
    "HTCCSMASTER" : {
        "ACCESS_KEY" : "AKIAJWTRPLDEVV3ZIL7A",
        "SECRET_KEY" : "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"
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
        self._all_instances[region] = region_info.get_only_instances(filters={"instance-state-name": "running"})
 
        if region not in self._all_instances.keys():
            return retVal
        if "vpc" in kwargs.keys():
            if kwargs["vpc"]:
                instances = [i for i in self._all_instances[region] if i.vpc_id is not None and i.state == "running"]
            elif not kwargs["vpc"]:
                instances = [i for i in self._all_instances[region] if i.vpc_id is None]
            kwargs.pop("vpc")
        else:
            instances = [ i for i in self._all_instances[region] if i.state == "running" ]
        args = dict([(k, kwargs[k]) for k in kwargs.keys() if hasattr(boto.ec2.instance.Instance, k) ])
 
        for zone in self._zones[region]:
            instances_in_zone = self.get_instance_counts_from_az(zone.name, instances)
            if len(instances_in_zone.keys()) > 0:
                retVal[zone.name] = instances_in_zone
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
        self.total_running_instance_unit_utilization = {}
        self.total_running_instance_utilization = {}
        self.total_reserved_instance_utilization = {}
        self.actual_reserved_instance_usage = {}
        self.unused_reserved_instance_count = {}
 
    def add_account_running_instance_utilization(self, account_name, utilization):
        self.accounts_utilization[account_name] = utilization
        #self.total_running_instance_utilization[account_name] = utilization
        #print "keys in utilization", utilization.keys()
        #print "keys in self.total_running_instance_utilization", self.total_running_instance_utilization.keys()
        for region in utilization.keys():
            self.total_running_instance_utilization = \
                self.add_dictionaries(
                    self.total_running_instance_utilization,
                    region,
                    utilization[region])
            #self.add_region_running_instance_utilization(region, region_utilization)
 
    def add_account_reserved_instance_utilization(self, account_name, utilization):
        #self.accounts_reserved_instance_utilization[account_name] = utilization
        for region in utilization.keys():
            region_utilization = utilization[region]
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
        ret_val = dict([(k, parent_dict[k]) for k in parent_dict.keys()])
        if child == {}:
            return ret_val
        if node_name in parent_dict.keys():
            if type(parent_dict[node_name]).__name__ == "dict":
                for sub_node_name in child.keys():
                    grandchild = child[sub_node_name]
                    ret_val[node_name] = self.add_dictionaries(ret_val[node_name], sub_node_name, grandchild)
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
        if region in self.total_running_instance_utilization.keys():
            return self.total_running_instance_utilization[region]
        return {}
 
    def get_running_instance_utilization_by_account(self, account_name):
        return self.accounts_utilization[account_name]
 
    def get_running_instance_units_totals(self, region):
        self.total_running_instance_unit_utilization[region] = {"totals" : {}}
 
        if region not in self.total_running_instance_utilization.keys():
            return self.total_running_instance_unit_utilization[region]
 
        #print self.total_running_instance_utilization[region].keys()
        for zone in self.total_running_instance_utilization[region].keys():
            self.total_running_instance_unit_utilization[region][zone] = {}
            instance_classes = list(set([type.split(".")[0] for type in self.total_running_instance_utilization[region][zone].keys()]))
            #print instance_classes
            for instance_class in instance_classes:
                self.total_running_instance_unit_utilization[region][zone][instance_class] = \
                    sum([self.convert_instance_type_to_units(instance_type, self.total_running_instance_utilization[region][zone][instance_type])
                        for instance_type in self.total_running_instance_utilization[region][zone].keys()
                            if instance_type.startswith(instance_class) ])
                try:
                    self.total_running_instance_unit_utilization[region]["totals"][instance_class] += self.total_running_instance_unit_utilization[region][zone][instance_class]
                except KeyError:
                    self.total_running_instance_unit_utilization[region]["totals"][instance_class] = self.total_running_instance_unit_utilization[region][zone][instance_class]
        print self.total_running_instance_unit_utilization[region]["totals"]
        return self.total_running_instance_unit_utilization[region]
 
    def convert_instance_type_to_units(self, instance_type, count):
        unit_normalization = {
            "micro" : 0.5,
            "small" : 1,
            "medium" : 2,
            "large" : 4,
            "xlarge" : 8,
            "2xlarge" : 16,
            "4xlarge" : 32,
            "8xlarge" : 64
        }
        return count * unit_normalization[instance_type.split(".")[1]]
 
 
 
if __name__ == "__main__":
    region = "ap-northeast-1"
    zone   = "ap-northeast-1a"
    instance_type = "m2.2xlarge"
    running_instances = {}
    reserved_instances = {}
    totals = ConsolidatedUtilization()
    actual = {}
    for name, account in ACCOUNTS.iteritems():
        #if name == "HTCCSMASTER":
        #    continue
        running_instances[name] = {}
        reserved_instances[name] = {}
        account = EC2Account(name, account["ACCESS_KEY"], account["SECRET_KEY"])
 
        for region in [r.name for r in boto.ec2.regions() if r.name.find("gov") == -1 and r.name.find("cn") == -1]:
            running_instances[name][region] = account.list_instances(region)
            #reserved_instances[name][region] = account.list_reserved_instances(region, vpc=True)
        #totals.add_account_reserved_instance_utilization(name, reserved_instances[name])
        #totals.add_account_running_instance_utilization(name, running_instances[name])
 
    with open("account_instance_utilization_totals" + ".csv", "w") as f:
        a = csv.writer(f)
        a.writerow(["Account", "Region", "Zone", "Instance Type", "Count"])
        for account, utilization in running_instances.iteritems():
            for region, region_utilization in utilization.iteritems():
                for zone, zone_utilization in region_utilization.iteritems():
                    for instance_type, count in zone_utilization.iteritems():
                        a.writerow([account, region, zone, instance_type, count])
            #print account, json.dumps(utilization)
 
    for name in running_instances.keys():
        account_utilization = running_instances[name]
        totals.add_account_running_instance_utilization(name, account_utilization)
        #print name, json.dumps(account_utilization)
    #for region in [r.name for r in boto.ec2.regions() if r.name.find("gov") == -1]:
    #    print "reserved", totals.get_reserved_instance_utilization_totals(region)
    #    print "running", totals.get_running_instance_utilization_totals(region)
 
 
    for name, account_utilization in running_instances.iteritems():
        region = "us-west-1"
        zone = region + "a"
        if region in account_utilization.keys() and zone in account_utilization[region].keys():
            print name, json.dumps(account_utilization[region][zone])
 
    with open("reserved_instance_totals-d" + ".csv", "w") as f:
        a = csv.writer(f)
        headers = ["Region", "Zone", "Instance Type"] + running_instances.keys() + ["Total Instances", "Total Units"]
        a.writerow(headers)
        for region in [r.name for r in boto.ec2.regions() if r.name.find("gov") == -1 and r.name.find("cn") == -1]:
            #actual[region] = totals.get_reserved_instance_usage(region)
            #print region, json.dumps(totals.get_running_instance_utilization_totals(region))
 
            #print region, json.dumps(totals.get_running_instance_units_totals(region))
            instance_units = totals.get_running_instance_units_totals(region)
            instance_totals = totals.get_running_instance_utilization_totals(region)
            #for zone in instance_units.keys():
            #    for instance_class in instance_units[zone].keys():
            #        a.writerow([region, zone, instance_class, instance_units[zone][instance_class]])
 
            for zone in instance_totals.keys():
                for instance_type in instance_totals[zone].keys():
                    print region, zone, instance_type
                    instance_type_count = {}
                    for name, account_utilization in running_instances.iteritems():
                        try:
                            instance_type_count[name] = account_utilization[region][zone][instance_type]
                        except:
                            instance_type_count[name] = 0
                    account_totals = [instance_type_count[name] for name in running_instances.keys()]
                    try:
                        unit_totals = instance_units["totals"][instance_type.split(".")[0]]
                    except KeyError:
                        unit_totals = 0
                    row = [region, zone, instance_type] + account_totals + [instance_totals[zone][instance_type], unit_totals]
                    a.writerow(row)
                    print row
    #print json.dumps(actual)
