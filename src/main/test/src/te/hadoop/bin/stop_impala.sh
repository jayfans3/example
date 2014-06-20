ps -ef|grep statestore |grep -v grep|awk '{print $2}' |xargs kill
