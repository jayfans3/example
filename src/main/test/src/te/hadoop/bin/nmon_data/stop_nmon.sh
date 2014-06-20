ps -ef|grep nmon|grep -v grep|grep 5000|awk '{print $2}'|xargs kill -9
