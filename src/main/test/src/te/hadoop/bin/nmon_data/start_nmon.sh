#!/bin/bash

if [ $# -lt 1 ]
then
  echo "Usage:  start_nmon.sh Command MachineTag"
  exit
fi
Host=`hostname`
FileName=${Host}_${1}.nmon
BINPATH=`dirname $0`
DATAPATH=$BINPATH/data
echo "Host=$Host FileName=$FileName BINPATH=$BINPATH; DATAPATH=$DATAPATH"
$BINPATH/nmon -s2 -c5000 -f -m$DATAPATH -F${FileName}
