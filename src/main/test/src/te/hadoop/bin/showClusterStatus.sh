#!/bin/bash
#set -x
# Description:  Show cluster status 
#               please refer to clusterProcess.conf for how to configure
#
# Author:       Zigao Wang <wangzg@asiainfo-linkage.com>

#       usage: $0 [confFile] 
#

 lineProc=2  ### process num displayed in one line 
 logTime1=600 ### less than this value treated as normal
 logTime2=1800 ###larger than this value treated as error else warning
 if [ $# -eq 1 ]
 then 
    confFile=$1
 else
    confFile=./clusterProcess.conf
 fi
 
 if [ -f $confFile ]
 then
    config=`cat $confFile`
    for line in $config
    do
            server=`echo $line|awk -F',' '{print $1}'`
            echoContent=""
            echoServer="\033[32;40;1m $server \033[0m"
            processList=`ssh $server ". ~/.bash_profile;jps"`
            correctProcessLog=`echo $line|awk -F',' '{for(i=2;i<=NF;i++) print $i}'`
            count=0
            prefixOrigin="###############################################"
            for processLog in $correctProcessLog
            do 
                if [ $count -ne 0 ] && [ `expr $count % $lineProc` -eq 0 ]
                then
                    prefixlen=`expr ${#server} + 12`
                    prefix="\033[30;40m""\r\n${prefixOrigin:0:$prefixlen}\033[0m"
                else
                    prefix="\033[30;40m######\033[0m"
                fi
                process=`echo $processLog|awk -F':' '{print $1}'`
                log=`echo $processLog|awk -F':' '{print $2}'`
                ifOK=false
                for actual in $processList
                do
                    if [ $process == $actual ]
                    then
                       ifOK=true
                       break
                    fi
                done
                if [ ! "$log" == 'UNNEEDED' ]
                then 
                    logTime=`ssh $server "ls --full-time $log"|awk '{indx=index($7,".");print $6" "substr($7,0,indx-1)}'`
                    timediff=$(($(date +%s)-$(date +%s -d "$logTime")))
                    if [ $timediff -lt $logTime1 ]
                    then
                       logContent="LOG:\033[38;42;1m$timediff SECONDS\033[0m"
                    else
                       if [ $timediff -gt $logTime2 ]
                       then
                          logContent="LOG:\033[38;41;1m$timediff SECONDS\033[0m"
                          echoServer="\033[31;40;1m $server \033[0m"
                       else
                          logContent="LOG:\033[31;43;1m$timediff SECONDS\033[0m"
                          echoServer="\033[31;40;1m $server \033[0m"
                       fi             
                    fi
                    if [ $ifOK == true ]
                    then
                       echoContent=$echoContent"$prefix"$process":\033[38;44;1m RUNNING \033[0m $logContent"
                    else
                       echoContent=$echoContent"$prefix"$process":\033[38;41;1m STOPPED \033[0m $logContent"
                       echoServer="\033[31;40;1m $server \033[0m"
                    fi
                else
                    #logContent="LOG MODIFIED \033[38;42;1m $timediff \033[0m SECONDS AGO"
                    if [ $ifOK == true ]
                    then
                       echoContent=$echoContent"$prefix"$process":\033[38;44;1m RUNNING \033[0m"
                    else
                       echoContent=$echoContent"$prefix"$process":\033[38;41;1m STOPPED \033[0m"
                       echoServer="\033[31;40;1m $server \033[0m"
                    fi
                fi
                count=$(($count+1))
            done
            echo -e  $echoServer"    "$echoContent
    done 
 else
   echo "Error: Please assign config file or run this command with clusterProcess.conf in same directory"
 fi
 
