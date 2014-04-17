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
echo $lib
hadoop fs -rmr /output/testbulkload

#~/app/mr1/bin/hadoop jar /home/ocdc/test/ocnosql/ocnosql-client-V01B05C00-SNAPSHOT/lib/ocnosql-client-V01B05C00-SNAPSHOT.jar singleimporttsv -Dimporttsv.columns=HBASE_ROW_KEY,f:c1,f:c2 -Dimporttsv.separator=; -Dimporttsv.bulk.output=hdfs://mycluster/zhuangyang/data datatsv hdfs://mycluster/zhuangyang/output

#shell agrs
tableName=$1
columns=$2
inputPath=$3
outputPath=$4
seperator=$5
algocolumn=$6
rowkeyGenerator=$7
rowkeycolumn=$8
callback=$9
#compressType=com.ailk.oci.ocnosql.client.compress.HbaseNullCompress
compressType=${10}
loadType=${11}

echo $columns

if[ $loadType=single ];then
  java -cp $lib com.ailk.oci.ocnosql.client.load.single.SingleColumnImportTsv -Dimporttsv.columns=${columns} -Dimporttsv.bulk.output=${outputPath}  -Dimporttsv.rowkeyGenerator=${rowkeyGenerator}  -Dimporttsv.algocolumn=${algocolumn} -Dimporttsv.callback=${callback} -Dimporttsv.rowkeycolumn=${rowkeycolumn} -DcompressType=$compressType ${tableName} ${inputPath}
else
  java -cp $lib com.ailk.oci.ocnosql.client.load.mutiple.MutipleColumnImportTsv -Dimporttsv.columns=${columns} -Dimporttsv.bulk.output=${outputPath} -DcompressType=$compressType -Dimporttsv.rowkeycolumn=${rowkeycolumn}  -Dimporttsv.rowkeyGenerator=${rowkeyGenerator} -Dimporttsv.algocolumn=${algocolumn} -Dimporttsv.callback=${callback} ${tableName} ${inputPath}
fi