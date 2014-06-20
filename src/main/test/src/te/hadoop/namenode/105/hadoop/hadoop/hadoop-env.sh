# Copyright 2011 The Apache Software Foundation
# 
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Hadoop-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

export JAVA_HOME=/home_app/ocnosql/app/java
export HADOOP_HOME=/home_app/ocnosql/app/hadoop
export HADOOP_PREFIX=${HADOOP_HOME}
export HADOOP_COMMON_HOME=${HADOOP_HOME}
export HADOOP_HDFS_HOME=${HADOOP_HOME}
export HADOOP_YARN_HOME=${HADOOP_HOME}
export PATH=$PATH:${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
export JAVA_HOME JAVA_BIN PATH CLASSPATH JAVA_OPTS
export HADOOP_LIB=${HADOOP_HOME}/lib
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_LIBRARY_PATH=${HADOOP_HOME}/lib/native
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${HADOOP_HOME}/lib/native

export HBASE_HOME=/home_app/ocnosql/app/hbase
export HADOOP_CLASSPATH=$HBASE_HOME/hbase-0.94.12-security.jar:$HADOOP_CLASSPATH
export HADOOP_CLASSPATH=$HBASE_HOME/hbase-0.94.12-security-tests.jar:$HADOOP_CLASSPATH
export HADOOP_CLASSPATH=$HBASE_HOME/lib/protobuf-java-2.4.0a.jar:$HADOOP_CLASSPATH
export HADOOP_CLASSPATH=$HBASE_HOME/conf:$HADOOP_CLASSPATH

# The java implementation to use.
export JAVA_HOME=${JAVA_HOME}

# The jsvc implementation to use. Jsvc is required to run secure datanodes.
#export JSVC_HOME=${JSVC_HOME}

export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"/etc/hadoop"}

# Extra Java CLASSPATH elements.  Automatically insert capacity-scheduler.
for f in $HADOOP_HOME/contrib/capacity-scheduler/*.jar; do
  if [ "$HADOOP_CLASSPATH" ]; then
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f
  else
    export HADOOP_CLASSPATH=$f
  fi
done


#====jmx config====GuoYK
export HADOOP_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote"
export HADOOP_NAMENODE_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS "
export HADOOP_SECONDARYNAMENODE_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS "
export HADOOP_DATANODE_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS "
export HADOOP_BALANCER_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
export HADOOP_JOBTRACKER_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS"
export HADOOP_TASKTRACKER_OPTS="$HADOOP_JMX_BASE -Dcom.sun.management.jmxremote $HADOOP_TASKTRACKER_OPTS"
#====jmx config end====GuoYK

# The maximum amount of heap to use, in MB. Default is 1000.
export HADOOP_HEAPSIZE=4096
#export HADOOP_NAMENODE_INIT_HEAPSIZE=""

# Extra Java runtime options.  Empty by default.
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true $HADOOP_CLIENT_OPTS"

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS="-Dhadoop.security.logger=${HADOOP_SECURITY_LOGGER:-INFO,RFAS} -Dhdfs.audit.logger=${HDFS_AUDIT_LOGGER:-INFO,NullAppender} $HADOOP_NAMENODE_OPTS -Xms20480M -Xmx20480M"
export HADOOP_DATANODE_OPTS="-Dhadoop.security.logger=ERROR,RFAS $HADOOP_DATANODE_OPTS -Xms8192M -Xmx8192M"

export HADOOP_SECONDARYNAMENODE_OPTS="-Dhadoop.security.logger=${HADOOP_SECURITY_LOGGER:-INFO,RFAS} -Dhdfs.audit.logger=${HDFS_AUDIT_LOGGER:-INFO,NullAppender} $HADOOP_SECONDARYNAMENODE_OPTS"

# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
HADOOP_JAVA_PLATFORM_OPTS="-XX:-UsePerfData $HADOOP_JAVA_PLATFORM_OPTS"

# On secure datanodes, user to run the datanode as after dropping privileges
export HADOOP_SECURE_DN_USER=${HADOOP_SECURE_DN_USER}

# Where log files are stored.  $HADOOP_HOME/logs by default.
#export HADOOP_LOG_DIR=${HADOOP_LOG_DIR}/$USER

# Where log files are stored in the secure data environment.
export HADOOP_SECURE_DN_LOG_DIR=${HADOOP_LOG_DIR}/${HADOOP_HDFS_USER}

# The directory where pid files are stored. /tmp by default.
# NOTE: this should be set to a directory that can only be written to by 
#       the user that will run the hadoop daemons.  Otherwise there is the
#       potential for a symlink attack.
export HADOOP_PID_DIR=${HADOOP_PID_DIR}
export HADOOP_SECURE_DN_PID_DIR=${HADOOP_PID_DIR}

# A string representing this instance of hadoop. $USER by default.
export HADOOP_IDENT_STRING=$USER
