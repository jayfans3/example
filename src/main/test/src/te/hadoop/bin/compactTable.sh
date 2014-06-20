#!/bin/bash

date1=`date -d "-1 day"  +%Y%m%d`
date2=`date -d "-3 day" +%Y%m%d`
echo $date1
echo $date2
table="dr_query"
tablename1="$table$date1"
while [ $date2 -lt $date1 ]
do
    date
    tableName="$table$date1"
    echo $tableName
    commond="compact '$tableName'"
    echo $commond

    echo "$commond"|/home/ocnosql/hbase/bin/hbase shell
#    sleep 3600
    date1=`date -d "-1 day $date1" +%Y%m%d`

done



#tableName=dr_query`date -d yesterday +%Y%m%d`
#if [ $# -eq 1 ]
#then
#   tableName=$1
#fi

#/home/ocnosql/hbase/bin/hbase shell <<THEEND
#$commond
#exit
#THEEND
