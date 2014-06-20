# Set Hadoop-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
# export JAVA_HOME=/usr/lib/j2sdk1.6-sun
export JAVA_HOME=/home_app/ocnosql/app/java
export HADOOP_HOME=/home_app/ocnosql/app/hadoop-mr1
export PATH=$PATH:${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
export JAVA_HOME PATH CLASSPATH 
export HADOOP_LIB=${HADOOP_HOME}/lib
export HADOOP_CONF_DIR=${HADOOP_HOME}/conf
export HADOOP_LIBRARY_PATH=${HADOOP_HOME}/lib/native
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${HADOOP_HOME}/lib/native

#export HBASE_HOME=/home_app/ocnosql/app/hbase
#export HADOOP_CLASSPATH=$HBASE_HOME/hbase-0.94.12-security.jar:$HADOOP_CLASSPATH
#export HADOOP_CLASSPATH=$HBASE_HOME/hbase-0.94.12-security-tests.jar:$HADOOP_CLASSPATH
#export HADOOP_CLASSPATH=$HBASE_HOME/lib/protobuf-java-2.4.0a.jar:$HADOOP_CLASSPATH
#export HADOOP_CLASSPATH=$HBASE_HOME/conf:$HADOOP_CLASSPATH

export HADOOP_HEAPSIZE=8192

#export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true $HADOOP_CLIENT_OPTS"
#
#export HADOOP_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote"
#export HADOOP_NAMENODE_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS"
#export HADOOP_SECONDARYNAMENODE_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS"
#export HADOOP_DATANODE_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS"
#export HADOOP_BALANCER_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
#export HADOOP_JOBTRACKER_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS"
#export HADOOP_TASKTRACKER_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_TASKTRACKER_OPTS"

# Extra Java CLASSPATH elements.  Optional.
# export HADOOP_CLASSPATH="<extra_entries>:$HADOOP_CLASSPATH"

# The maximum amount of heap to use, in MB. Default is 1000.
# export HADOOP_HEAPSIZE=2000

# Extra Java runtime options.  Empty by default.
# if [ "$HADOOP_OPTS" == "" ]; then export HADOOP_OPTS=-server; else HADOOP_OPTS+=" -server"; fi

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS"
export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS"
export HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
export HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote -Xms10240M -Xmx10240M $HADOOP_JOBTRACKER_OPTS"
export HADOOP_TASKTRACKER_OPTS="-Dcom.sun.management.jmxremote -Xms8192M -Xmx8192M $HADOOP_TASKTRACKER_OPTS"
# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
# export HADOOP_CLIENT_OPTS

# Extra ssh options.  Empty by default.
# export HADOOP_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HADOOP_CONF_DIR"

# Where log files are stored.  $HADOOP_HOME/logs by default.
# export HADOOP_LOG_DIR=${HADOOP_HOME}/logs

# File naming remote slave hosts.  $HADOOP_HOME/conf/slaves by default.
# export HADOOP_SLAVES=${HADOOP_HOME}/conf/slaves

# host:path where hadoop code should be rsync'd from.  Unset by default.
# export HADOOP_MASTER=master:/home/$USER/src/hadoop

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HADOOP_SLAVE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
# export HADOOP_PID_DIR=/var/hadoop/pids

# A string representing this instance of hadoop. $USER by default.
# export HADOOP_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export HADOOP_NICENESS=10
