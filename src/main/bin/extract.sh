#! /bin/sh
OCNOSQL_CLIENT_HOME=`cd ..;pwd`
OCNOSQL_CLIENT_HOME=$OCNOSQL_CLIENT_HOME
export OCNOSQL_CLIENT_HOME

for f in $OCNOSQL_CLIENT_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

CLASSPATH=$OCNOSQL_CLIENT_HOME/conf:${CLASSPATH}
export CLASSPATH
CLASSPATH=${CLASSPATH}

java -server -Xms10240m -Xmx10240m -XX:MaxPermSize=25600M -XX:-UseGCOverheadLimit -cp $CLASSPATH com.ailk.oci.ocnosql.client.importdata.Extract $1 $2 $3 $4 