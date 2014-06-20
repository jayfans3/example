#!/bin/sh
set -x
type=("GSM")

for t in ${type[*]} ; do

    for i in {1..9} ; do
	echo i
#        hadoop fs -mkdir /data/${t}/day=2013040${i}
#        hadoop fs -mv /ftpdata/201304/${t}_2013040${i}* /data/${t}/day=2013040${i}

    done

    #for k in {10..15} ; do
    #    hadoop fs -mkdir /data/${t}/day=201304${k}
    #    hadoop fs -mv /ftpdata/201304/${t}_201304${k}* /data/${t}/day=201304${k}
    #done

    for j in {16..31} ; do
        #hadoop fs -mkdir /data/${t}/day=201303${j}
        hadoop fs -mv /ftpdata/201303/${t}/201303${j}/* /data/${t}/day=201303${j}
    done

done
