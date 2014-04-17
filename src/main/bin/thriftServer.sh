lib=~/cdh43/hadoop-2.0.0-mr1-cdh4.2.1/conf:~/cdh43/hbase-0.94.6-cdh4.3.0/conf

#for i in /home/ocnosql/mobiletest/*.jar;do
#    lib=$lib:$i
#done
for i in /home/ocnosql/cdh43/hadoop-2.0.0-cdh4.3.0/share/hadoop/*/*.jar;do
        lib=$lib:$i
done
#oldClientJar=ocnosql-client-V01B05C00-SNAPSHOT.jar
for i in ~/cdh43/hadoop-2.0.0-mr1-cdh4.2.1/lib/*.jar; do
    #c=`echo $i|grep $oldClientJar|wc -l`
compressType=${10}


    #if [ $c -ne 1 ]
    #then
        lib=$lib:$i
#java -cp $lib -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8071 com.ailk.oci.ocnosql.client.load.mutiple.MutipleColumnImportTsvTest $tab
    #fi
    #echo `ls i | grep *.jar`
done

for i in ~/cdh43/hadoop-2.0.0-mr1-cdh4.2.1/*.jar; do
    lib=$lib:$i
done

for i in /home/ocnosql/app/hbase-0.94.12-security/*.jar; do
    lib=$lib:$i
done
#for i in ~/cdh43/hbase-0.94.6-cdh4.3.0/conf/*; do
#    lib=$lib:$i
#done


java -cp $lib com.ailk.oci.ocnosql.client.thrift.server.ThriftServer

