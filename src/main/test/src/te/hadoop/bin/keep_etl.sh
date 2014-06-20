#!/bin/bash
#startup script for etl on linux heartbeat
#filename etl.sh

################################################
JAVA_HOME=/home_app/ocnosql/app/java
TOMCAT_HOME=/home_app/ocnosql/app/etl
export JAVA_HOME
export TOMCAT_HOME
etlpid=`ps -ef|grep etl|grep ocnosql|grep Bootstrap|grep -v grep|awk '{print $2}'`
etlServerpid=`${JAVA_HOME}/bin/jps|grep OCEtlServer|grep -v Jps|awk '{print $1}'`
MQpid=`${JAVA_HOME}/bin/jps|grep JMSClientRunner|grep -v Jps|awk '{print $1}'`
vIP=`/sbin/ifconfig |grep 10.4.52.110|wc -l`
################################################
start_tomcat=$TOMCAT_HOME/bin/startup.sh
stop_tomcat=$TOMCAT_HOME/bin/shutdown.sh
stop_server=/home_app/ocnosql/app/schedule/bin/stopMQClient.sh
stop_MQ=/home_app/ocnosql/app/schedule/bin/stopOCetlServer.sh
################################################
echo -n `date +"%Y-%m-%d %H:%M:%S"` "  "

start() {
    echo -n "Starting etl: "
    ${start_tomcat}
    cd /home_app/ocnosql/app/schedule/bin; ./startMQClient.sh; ./startOCetlServer.sh
    echo "etl start [OK]"
}
################################################
stop() {
    echo -n "Shutdown etl"
    ${stop_tomcat}
    ${stop_server}
    ${stop_MQ}
    kill -9 ${etlpid} ${etlServerpid} ${MQpid}
    echo "etl stop [OK]"
}
################################################

if [ "x${vIP}" == x1 ] ; then
        if [ "x$1" == "xstart" ] ; then
                if [ "x${etlpid}${etlServerpid}${MQpid}" == "x" ] ; then
                        start
                else
                        echo "etl is running"
                fi
        elif [ "x$1" == "xstop" ] ; then
                stop
        elif [ "x$1" == "xrestart" ] ; then
                stop
                echo "sleep 10 second"
                sleep 10
                start
        else
                echo "Usage: $0 {start|stop|restart}"   
        fi
else
        echo "vIP is not in this machine"
fi
