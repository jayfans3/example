IP_Addr=("10.9.141.150" "10.9.141.151" "10.9.141.152" "10.9.141.153" "10.9.141.154" "10.9.141.155" "10.9.141.156" "10.9.141.157" "10.9.141.158" "10.9.141.159" "10.9.141.160" "10.9.141.161" "10.9.141.165" "10.9.141.166" "10.9.141.167" "10.9.141.168" "10.9.141.169")

fileName=datanodeblock_monitor.log.`date -u +%Y%m%d-%H:%M`
filePath=/home/ocnosql/ocnosql/log/datanode
destPath=/home/ocnosql/

cd $filePath
for value in ${IP_Addr[*]} ; do
        ssh $value "source ~/.bash_profile; jps | grep DataNode | awk '{print \$1}' | xargs jstack" > datanode-stack-$value.log
        cnt=`cat datanode-stack-$value.log|grep -i blocked|wc -l`
        if [ $cnt > 0 ] ; then
                Tdate=` date +"%Y-%m-%d %H:%M:%S" `
                echo "XDYCX;DataNodeblocked;" $Tdate";"$IP_Addr"; datanode have dead lock!!; jstack datanodeID appear blocked" > $fileName.$value
#               ftp -inv 10.10.140.166 <<hbck
#               quote user boms30
#               quote pass asiainfo
#               lcd ${filePath}
#               cd ${destPath}
#               put ${fileName}.$value
#               by
#hbck
        fi

        rm  datanode-stack-$value.log

done