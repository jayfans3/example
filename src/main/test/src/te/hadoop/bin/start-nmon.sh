if [ $# -lt 2 ]
then
  echo "Usage:  start_nmon.sh Command MachineTag"
  exit
fi
FileName=$1
runRemoteCmd.sh "cd /home/ocnosql/bin/nmon_data; ./start_nmon.sh $FileName" $2
