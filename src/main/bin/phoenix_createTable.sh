#! /bin/bash
#this shell script to create table in phoenix ,the table is be mapped to hbase table.
#NOTE: \n in unix.

OCNOSQL_CLIENT_HOME=`cd ..;pwd`
OCNOSQL_CLIENT_HOME=$OCNOSQL_CLIENT_HOME
export OCNOSQL_CLIENT_HOME

for f in $OCNOSQL_CLIENT_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

CLASSPATH=$OCNOSQL_CLIENT_HOME/conf:${CLASSPATH}
export CLASSPATH
CLASSPATH=${CLASSPATH}

#args example : YUJINGTEST ID F.NAME,F.AGE,F.TEL,F.SEX,F.ADDR,F.DATE

java -server -Xms512m -Xmx512m -XX:MaxPermSize=512M -XX:-UseGCOverheadLimit -cp $CLASSPATH com.ailk.oci.ocnosql.client.importdata.phoenix.PhoenixCreate $1 $2 $3