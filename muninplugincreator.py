#!/usr/bin/env python
__author__ = 'facundo_nishiwaki'
 
import boto.ec2
from boto.ec2 import instance
import json
import os
import ConfigParser
import argparse
 
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
 
class EC2Account:
    def __init__(self, name, access_key, secret_key):
        self.name = name
        self._access_key = access_key
        self._secret_key = secret_key
        self._instance_utilization = {}
        self._all_instance_types = {}
        self._all_instances = {}
        self._all_reserved_instances = {}
        self._zones = {}
 
    def connect(self, region):
        region_info = boto.ec2.connect_to_region(region,
                                                 aws_access_key_id=self._access_key,
                                                 aws_secret_access_key=self._secret_key)
        self._zones[region] = region_info.get_all_zones()
        instances = region_info.get_only_instances()
        reserved_instances = region_info.get_all_reserved_instances()
        self._all_instances[region] = instances
        self._all_instance_types[region] = list(set([i.instance_type for
                                                     i in instances] + [i.instance_type for i in reserved_instances]))
        self._zones[region] = region_info.get_all_zones()
 
    def all_instance_types(self, region):
        return self._all_instance_types[region]
 
    def all_zones(self, region):
        return  self._zones[region]
 
class MyConfigParser(ConfigParser.ConfigParser):
    def write(self, fp):
        """Write an .ini-format representation of the configuration state."""
        if self._defaults:
            fp.write("[%s]\n" % DEFAULTSECT)
            for (key, value) in self._defaults.items():
                fp.write("%s : %s\n" % (key, str(value).replace('\n', '\n\t')))
            fp.write("\n")
        for section in self._sections:
            fp.write("[%s]\n" % section)
            for (key, value) in self._sections[section].items():
                if key != "__name__":
                    fp.write("%s %s\n" %
                             (key, str(value).replace('\n', '\n\t')))
            fp.write("\n")
 
class MuninPluginCreator:
    def __init__(self, region):
        self._region = region
        self._plugins_dir = "/etc/munin/plugins/"
        self._plugins_environment_config_file = "/etc/munin/plugin-conf.d/reserved-instances"
        self._plugin_filename = "/etc/munin/plugins/muninplugin.py"
        self._munin_configurations = {}
        self.all_instance_types = []
        self.zones = []
        self.accounts  = ACCOUNTS
        self.get_region_info()
 
    def get_region_info(self):
        for name, account in self.accounts.iteritems():
            ec2account = EC2Account(name, account["ACCESS_KEY"], account["SECRET_KEY"])
            region = self._region
            ec2account.connect(region)
            self.zones += [zone.name for zone in ec2account.all_zones(region)]
            self.all_instance_types += ec2account.all_instance_types(region)
        self.zones = list(set(self.zones))
        self.all_instance_types = list(set(self.all_instance_types))
 
    def create_plugin(self, source_plugin_file, region, zones, instance_type):
 
        filename = instance_type
        try:
            os.symlink(source_plugin_file, self._plugins_dir + filename)
        except OSError, e:
            print "source file:", source_plugin_file
            print "filename:", filename
        self.create_environment_variables(filename, region, zones, instance_type)
 
    def create_environment_variables(self, plugin_filename, region, zones, instance_type):
        self._munin_configurations[plugin_filename] = {
            "REGION" : region,
            "ZONES"   : ",".join(zones),
            "INSTANCETYPE" : instance_type
        }
 
    def save_munin_environment_configs(self):
        with open(self._plugins_environment_config_file,'w') as f:
            parser=MyConfigParser()
            for plugin_name, plugin_configs in self._munin_configurations.iteritems():
                parser.add_section(plugin_name)
                for key, value in plugin_configs.iteritems():
                    parser.set(plugin_name, "env." + key, value)
            parser.write(f)
 
    def run(self):
        region = self._region
        for type in self.all_instance_types:
            self.create_plugin(self._plugin_filename, region, self.zones, type)
        self.save_munin_environment_configs()
 
if __name__ == "__main__":
 
    parser = argparse.ArgumentParser(description='Create munin plugins based on reserved instance data')
    parser.add_argument('-r', '--region', help='AWS region code', required=True)
    args = vars(parser.parse_args())
    munin_plugins = MuninPluginCreator(args["region"])
