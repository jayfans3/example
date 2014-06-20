#!/bin/bash
#author benn-cs@ailk

cat << EOF
+---------------------------------------+
|   Auto Mount Deveice  |
|   		    Enjoy 		                  |
+---------------------------------------
EOF



devices=("b" "c" "d" "e" "f" "g" "h" "i" "j")

for device in ${devices[*]} ; do
echo $device

if [ -b /dev/sd$device ]
then
   tune2fs -l /dev/sd$device  1>& /dev/null 
   if [ $? == 0 ] 
    then
   	echo "!-------Formated-------------!"
   else    
	echo "!-------Need Format----------!"
        echo "!-------Check partion--------!"
	if [ -b /dev/sd$device"1" ]
          then
            echo "!-------Formated-------------!"
          else
            echo "!-------Format----------!"

          fi
	
	#mkfs -t ext4 /dev/sdb
   fi
else
  echo "dose not exist /dev/sd"$device
fi

done
