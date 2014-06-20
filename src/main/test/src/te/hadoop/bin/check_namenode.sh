#!/bin/bash
#¼ì²éDataNode

#app/zookeeper/bin/zkServer.sh start
#app/hadoop/sbin/hadoop-daemon.sh start namenode
#app/hadoop/sbin/hadoop-daemon.sh start journalnode
#app/hadoop/sbin/hadoop-daemon.sh start zkfc
#app/hadoop-mr1/bin/hadoop-daemon.sh start jobtrackerha
#app/hadoop-mr1/bin/hadoop-daemon.sh start mrzkfc

/home/ocnosql/app/jdk7/bin/jps  | grep -w NameNode | grep -v grep 1>& /dev/null 
if [ $? == 0 ] 
 then
    echo "!-------NameNode is ok-------------!" >> /tmp/log/namenode-check-online.log 
else    
	echo "!-------NameNode is dead ----------!" | tee -a /tmp/log/namenode-check-online.log   
	echo "Check date:`date '+%Y-%m-%d %H:%M:%S'`" | tee -a  /tmp/log/namenode-check-online.log    
	echo "!-------NameNode  Restart----------!" | tee -a /tmp/log/namenode-check-online.log   
	# Restart
	cd /home/ocnosql/app/hadoop/sbin
	./hadoop-daemon.sh start namenode
	echo "!-------DataNode  OK    -----------!" | tee -a /tmp/log/datanode-check-online.log 
fi


#¼ì²é JournalNode
/home/ocnosql/app/jdk7/bin/jps  | grep -w JournalNode | grep -v grep 1>& /dev/null 
if [ $? == 0 ] 
then
    echo "!-------JournalNode is ok-------------!" >> /tmp/log/JournalNode-check-online.log 
else    
	echo "!-------JournalNode is dead ----------!" | tee -a /tmp/log/JournalNode-check-online.log   
	echo "Check date:`date '+%Y-%m-%d %H:%M:%S'`" | tee -a  /tmp/log/JournalNode-check-online.log    
	echo "!-------JournalNode Restart----------!" | tee -a /tmp/log/JournalNode-check-online.log   
	# Restart
	cd /home/ocnosql/app/hadoop/sbin 
	./hadoop-daemon.sh start journalnode 
	echo "!-------JournalNode OK    -----------!" | tee -a /tmp/log/JournalNode-check-online.log 
fi

#JobTrackerHADaemon

/home/ocnosql/app/jdk7/bin/jps  | grep -w JobTrackerHADaemon | grep -v grep 1>& /dev/null 
if [ $? == 0 ] 
then
    echo "!-------JobTrackerHADaemon is ok-------------!" >> /tmp/log/JobTrackerHADaemon-check-online.log    
else
	echo "!-------JobTrackerHADaemon is dead ----------!" | tee -a /tmp/log/JobTrackerHADaemon-check-online.log   
	echo "Check date:`date '+%Y-%m-%d %H:%M:%S'`" | tee -a  /tmp/log/JobTrackerHADaemon-check-online.log    
	echo "!-------JobTrackerHADaemon Restart ----------!" | tee -a /tmp/log/JobTrackerHADaemon-check-online.log   
	# Restart
	cd /home/ocnosql/app/hadoop-mr1/bin
	./hadoop-daemon.sh start jobtrackerha
	echo "!-------JobTrackerHADaemon OK     -----------!" | tee -a /tmp/log/JobTrackerHADaemon-check-online.log 
fi

/home/ocnosql/app/jdk7/bin/jps  | grep -w DFSZKFailoverController | grep -v grep 1>& /dev/null
if [ $? == 0 ]
then
    echo "!-------DFSZKFailoverController is ok-------------!" >> /tmp/log/DFSZKFailoverController-check-online.log 
else
        echo "!-------DFSZKFailoverController is dead ----------!" | tee -a /tmp/log/DFSZKFailoverController-check-online.log
        echo "Check date:`date '+%Y-%m-%d %H:%M:%S'`" | tee -a  /tmp/log/DFSZKFailoverController-check-online.log
        echo "!-------DFSZKFailoverController Restart ----------!" | tee -a /tmp/log/DFSZKFailoverController-check-online.log
        # Restart
        cd /home/ocnosql/app/hadoop/sbin
        ./hadoop-daemon.sh start zkfc
        echo "!-------DFSZKFailoverController OK     -----------!" | tee -a /tmp/log/DFSZKFailoverController-check-online.log
fi 

#MRZKFailoverController
/home/ocnosql/app/jdk7/bin/jps  | grep -w MRZKFailoverController| grep -v grep 1>& /dev/null 
if [ $? == 0 ] 
then
    echo "!-------MRZKFailoverController is ok-------------!" >> /tmp/log/MRZKFailoverController-check-online.log 
else    
        echo "!-------MRZKFailoverController is dead ----------!" | tee -a /tmp/log/MRZKFailoverController-check-online.log   
        echo "Check date:`date '+%Y-%m-%d %H:%M:%S'`" | tee -a  /tmp/log/MRZKFailoverController-check-online.log    
        echo "!-------MRZKFailoverController Restart----------!" | tee -a /tmp/log/MRZKFailoverController-check-online.log   
        # Restart
        cd /home/ocnosql/app/hadoop-mr1/bin 
        ./hadoop-daemon.sh start mrzkfc
        echo "!-------MRZKFailoverController OK    -----------!" | tee -a /tmp/log/MRZKFailoverController-check-online.log 
fi

#QuorumPeerMain
/home/ocnosql/app/jdk7/bin/jps  | grep -w QuorumPeerMain| grep -v grep 1>& /dev/null 
if [ $? == 0 ] 
then
    echo "!-------QuorumPeerMain is ok-------------!" >> /tmp/log/QuorumPeerMain-check-online.log 
else    
        echo "!-------QuorumPeerMain is dead ----------!" | tee -a /tmp/log/QuorumPeerMain-check-online.log   
        echo "Check date:`date '+%Y-%m-%d %H:%M:%S'`" | tee -a  /tmp/log/QuorumPeerMain-check-online.log    
        echo "!-------QuorumPeerMain Restart----------!" | tee -a /tmp/log/QuorumPeerMain-check-online.log   
        # Restart
        #source /home/ocnoql/.bash_profile
	export JAVA_HOME=/home/ocnosql/app/jdk7
	cd /home/ocnosql/app/zookeeper/bin/
	#./zkServer.sh stop
	#echo "!-------QuorumPeerMain STOP OK ---------!" |  tee -a /tmp/log/QuorumPeerMain-check-online.log
	./zkServer.sh start 
        echo "!-------QuorumPeerMain OK    -----------!" | tee -a /tmp/log/QuorumPeerMain-check-online.log 
fi

#HMaster
/home/ocnosql/app/jdk7/bin/jps  | grep -w HMaster| grep -v grep 1>& /dev/null
if [ $? == 0 ]
then
    echo "!-------HMaster is ok-------------!" >> /tmp/log/HMaster-check-online.log
else
        echo "!-------HMaster is dead ----------!" | tee -a /tmp/log/HMaster-check-online.log
        echo "Check date:`date '+%Y-%m-%d %H:%M:%S'`" | tee -a  /tmp/log/HMaster-check-online.log
        echo "!-------HMaster Restart----------!" | tee -a /tmp/log/HMaster-check-online.log
        # Restart
        #source /home/ocnoql/.bash_profile
        cd /home/ocnosql/app/hbase/bin/
        ./hbase-daemon.sh start master
	echo "!-------HMaster OK    -----------!" | tee -a /tmp/log/HMaster-check-online.log
fi

exit 0
