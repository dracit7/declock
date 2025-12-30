#!/bin/bash

source ~/set-env.sh

host=val10
proc=`ssh $host ps -ef | grep "microbench cn" | head -n 1`
pid=`echo "$proc" | awk '{print $2}'`
ppid=`echo "$proc" | awk '{print $3}'`
mid=`ssh $host ps -ef | grep "$ppid" | head -n 1 | awk '{print $10}'`

echo "Failing CN $mid"
echo "Process: $proc"
ssh val09 $DMLOCK_PATH/build/report_failure 127.0.0.1 $mid
ssh $host "kill -9 $pid"

