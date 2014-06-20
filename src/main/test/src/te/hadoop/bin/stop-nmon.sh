#!/bin/sh
#set -x
source ~/.bash_profile;
cd bin;
./runRemoteCmd.sh "/home/ocnosql/bin/nmon_data/stop_nmon.sh" all

runRemoteCmd.sh "ps -ef|grep nmon|grep -v grep|grep 5000|awk '{print \$2}' |xargs kill -9" all
