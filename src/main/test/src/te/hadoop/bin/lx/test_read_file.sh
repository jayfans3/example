file="/test/713/file_20G"
rm ./file_20G
#sdate=`date`
#echo "$sdate begin get $file to local "
cd ~/app/hadoop/bin;
./hdfs dfs -get $file ~/bin/lx/ &
#edate=`date`
#echo "$edate end get $file to local " date
