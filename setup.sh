#!/usr/bin/env bash
 
if [[ $(/usr/bin/id -u) -ne 0 ]]; then
    echo "Not running as root"
    exit
fi
 
git clone https://github.com/quietlycundo/ri-monitoring.git
cd ./ri-monitoring/
rm -rf /etc/munin/plugins/*
rm /tmp/_cache
chmod +x ./*.py
cp instanceutilization.py /etc/munin/plugins/
cp muninplugin.py /etc/munin/plugins/
./muninplugincreator.py --region $1
/etc/init.d/munin-node restart
