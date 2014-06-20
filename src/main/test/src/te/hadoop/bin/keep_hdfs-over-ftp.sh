#!/bin/bash
#set -x
#startup script for hdfs-over-ftp linux heartbeat
#filename lx_startHdfsOverFtp.sh
################################################
JAVA_HOME=/home_app/ocnosql/app/java
HDFS_OVER_FTP_HOME=/home_app/ocnosql/app/hdfs-over-ftp
export JAVA_HOME
export HDFS_OVER_FTP_HOME
hdfsoverftpid=`ps -ef|grep HdfsOverFtpServer|grep java|grep ocnosql|grep -v grep|awk '{print $2}'`
vIP=`/sbin/ifconfig |grep 10.4.52.110|wc -l`
################################################
start_hdfs_ftp="cd $HDFS_OVER_FTP_HOME;./hdfs-over-ftp.sh start"
stop_hdfs_ftp="$HDFS_OVER_FTP_HOME/hdfs-over-ftp.sh stop"
################################################
echo -n `date +"%Y-%m-%d %H:%M:%S"` "  "
start() {
    cd $HDFS_OVER_FTP_HOME
    echo -n "Starting hdfs-over-ftp: "
    ${stop_hdfs_ftp}
    cd $HDFS_OVER_FTP_HOME;./hdfs-over-ftp.sh start
#    ${start_hdfs_ftp}
    echo "hdfs-over-ftp start [OK]"
}
################################################
stop() {
    echo -n "Shutdown hdfs-over-ftp"
    ${stop_hdfs_ftp}
    kill -9 ${hdfsoverftpid}
    echo "hdfs-over-ftp stop [OK]"
}
################################################

if [ "x${vIP}" == x1 ] ; then
        if [ "x$1" == "xstart" ] ; then
                if [ "x${hdfsoverftpid}" == "x" ] ; then
                        start
                else
                        echo "HDFS-OVER-FTP is running"
                fi
        elif [ "x$1" == "xstop" ] ; then
                stop
                kill -9 $hdfsoverftpid
        elif [ "x$1" == "xrestart" ] ; then
                stop
                kill -9 $hdfsoverftpid
                echo "sleep 10 second"
                sleep 10
                start
        else
                echo "Usage: $0 {start|stop|restart}"   
        fi
else
        echo "vIP is not in this machine"
fi
