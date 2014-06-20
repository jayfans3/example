IP_Addr="10.9.141.156"
fileName=hbck_monitor.log.`date -u +%Y%m%d-%H:%M`
filePath=/home/ocnosql/ocnosql/log/hbase
destPath=/home/ocnosql/

source ~/.bash_profile
cd $filePath
hbase hbck > hbase_hbck.log

incons=`cat hbase_hbck.log |grep "inconsistencies detected"|awk '{print $1}'`
hStatus=`cat hbase_hbck.log |grep "Status:"|awk '{print $2}'`
echo "incons = $incons    and hStatue= $hStatus"

if [ $incons -ge 1 ] || [ $hStatus != 'OK'  ] ; then
        Tdate=` date +"%Y-%m-%d %H:%M:%S" `
        echo "XDYCX;hbase_hbck;" $Tdate";"$IP_Addr"; hbase service is unavilable!!; hbse hbck Statue = $hStatus, and $incons inconsistencies detected" > $fileName.$IP_Addr

#        ftp -inv 10.10.140.166 <<hbck
#        quote user boms30
#        quote pass asiainfo
#        lcd ${filePath}
#        cd ${destPath}
#        put ${fileName}.$IP_Addr.csv
#        by
#hbck

#rm ${fileName}.$IP_Addr.csv
fi
