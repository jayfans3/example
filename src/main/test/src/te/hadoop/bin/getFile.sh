#!/bin/bash
#set -x

if [ $# -lt 4 ]
then 
  echo "Usage: ./getFile.sh srcFile(or Dir) descFile(or Dir) MachineTag"
  echo "Usage: ./getFile.sh srcFile(or Dir) descFile(or Dir) MachineTag confFile"
  exit 
fi
master=`hostname`
src=$1
dest=$2
filename=$3
tag=$4

if [ 'a'$5'a' == 'aa' ]
then
  confFile=~/bin/deploy.conf
else 
  confFile=$4
fi

if [ -f $confFile ]
then
  if [ -d $dest ]
  then
    for server in `cat $confFile|grep -v '^#'|grep ','$tag','|awk -F',' '{print $1}'` 
    do
       echo "**************************$server******************************"
       #ssh $server "scp $src/$filename ${master}:$dest"
       #ssh $server "scp $src/$filename ${master}:$dest"
       scp ${server}:$src/$filename $dest
    done 
  else
      echo "Error: No Dest file exist"
  fi

else
  echo "Error: Please assign config file or run deploy.sh command with deploy.conf in same directory"
fi

