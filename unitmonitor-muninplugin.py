#!/usr/bin/env python
 
__author__ = 'facundo_nishiwaki'
 
 
import os
import os.path, time, stat
from munin import MuninPlugin
from instanceutilization import EC2Account
from instanceutilization import ConsolidatedUtilization
 
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
 
"""Munin Plugin"""
class AggregateReservedInstanceUtilization(MuninPlugin):
    def __init__(self, region, zones, class_type):
        title_pretext = class_type
        self.title = title_pretext + " - Remaining Reserved Instance Capacity"
        self.args  = "--base 1000 -l -15 -u 15"
        self.vlabel = "Idle Reserved Instance"
        self.scale = False
        self.category = "reserved_instances"
        self._region = region
        self._zones = zones
        self._class_type = class_type
        self._cache_file = "/tmp/_cache_class"
 
    @property
    def fields(self):
        utilization_fields = [self.create_utilization_field(z, self._class_type) for z in self._zones]
        ri_totals_fields = [self.create_reserved_instance_totals_field(z, self._class_type) for z in self._zones]
        running_totals_fields = [self.create_running_instance_totals_field(z, self._class_type) for z in self._zones]
        return utilization_fields + ri_totals_fields + running_totals_fields
 
    def create_utilization_field(self, zone, class_type):
        warning = -1
        critical = -5
        field = (zone + "_unused_total", dict(
                label = " ".join(["(", zone, ")", "unused", class_type, "RIs"]),
                info = zone + ' - Number of un-used purchased reserved instances: ' + class_type,
                draw = "LINE1",
                type = "GAUGE",
                warning = str(warning) + ":",
                critical = str(critical) + ":"))
        return field
 
    def create_reserved_instance_totals_field(self, zone, class_type):
        return (zone + "_ri_total", dict(
                label = " ".join(["(", zone, ")", class_type, "RIs"]),
                info = zone + ' - Number of purchased reserved instances: ' + class_type,
                draw = "LINE2",
                type = "GAUGE"))
 
    def create_running_instance_totals_field(self, zone, class_type):
        return (zone + "_running_total", dict(
                label = " ".join(["(", zone, ")", class_type, "running instances"]),
                info = zone + ' - Number of running instances: ' + class_type,
                draw = "LINE3",
                type = "GAUGE"))
 
    def execute(self):
        region = self._region
        class_type = self._class_type
        running_instances = {}
        reserved_instances = {}
        totals = ConsolidatedUtilization()
        actual = {}
 
        # Use cached data in /tmp if we cached it less than 9 minutes ago
        if os.path.exists(self._cache_file) and time.time() - os.stat(self._cache_file)[stat.ST_MTIME] < 60 * 9:
            cached = {}
            execfile(self._cache_file, cached)
            ri_usage = cached["ri_utilization"]
            ri_totals = cached["ri_totals"]
            running_totals = cached["running_totals"]
            unused_totals = cached["unused_totals"]
        else:
            for name, account in ACCOUNTS.iteritems():
                running_instances[name] = {}
                reserved_instances[name] = {}
                account = EC2Account(name, account["ACCESS_KEY"], account["SECRET_KEY"])
 
                #for region in [r.name for r in boto.ec2.regions() if r.name.find("gov") == -1]:
                running_instances[name][region] = account.list_instances(region)
                reserved_instances[name][region] = account.list_reserved_instances(region)
 
                totals.add_account_reserved_instance_utilization(name, reserved_instances[name])
                totals.add_account_running_instance_utilization(name, running_instances[name])
            ri_usage = totals.get_reserved_instance_usage(region)
            ri_totals = totals.get_reserved_instance_utilization_totals(region)
            running_totals = totals.get_running_instance_utilization_totals(region)
            unused_totals = totals.get_unused_reserved_instance_count(region)
            with open(self._cache_file,'w') as f:
                f.write("ri_utilization=" + str(ri_usage) + "\n")
                f.write("ri_totals=" + str(str(totals.get_reserved_instance_utilization_totals(region))) + "\n")
                f.write("running_totals=" + str(totals.get_running_instance_utilization_totals(region)) + "\n")
                f.write("unused_totals=" + str(totals.get_unused_reserved_instance_count(region)) + "\n")
                f.close()
        actual[region] = ri_usage
        reserved_class_type_totals = dict([(z + "_ri_total",ri_totals[z][class_type])
                                               for z in self._zones
                                               if class_type in ri_totals[z].keys()])
 
        running_class_type_totals = dict([(z + "_running_total",running_totals[z][class_type])
                                             for z in self._zones
                                             if class_type in running_totals[z].keys()])
        unused_class_type_totals = dict([(z + "_unused_total", unused_totals[z][class_type])
                                             for z in self._zones
                                             if class_type in unused_totals[z].keys()])
 
        utilization_totals = dict([ (z + "_unused_total", actual[region][z][class_type])
                      for z in actual[region].keys()
                        if class_type in actual[region][z].keys()])
 
        return dict(reserved_class_type_totals.items() +
                    utilization_totals.items() +
                    running_class_type_totals.items() +
                    unused_class_type_totals.items())
 
if __name__ == "__main__":
 
    region = os.environ.get('region')
    zones = os.environ.get('zones').split(",")
    class_type = os.environ.get('instancetype')
    #region = "us-east-1"
    #zones = ["us-east-1a", "us-east-1b"]
    #class_type = "c1.xlarge"
    AggregateReservedInstanceUtilization(region, zones, class_type).run()
