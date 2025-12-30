#!/bin/bash

script_dir=$(dirname "${0}")
source $script_dir/set-env.sh
source $script_dir/utils.sh

signal=$1

i=0
for host in `echo $HOSTS | tr ' ' '\n' | awk -F'-' '{print $1}' | uniq`; do
  ssh $host "
    pkill -${signal:=9} dmlock_;
    pkill -${signal:=9} dslr_;
    pkill -${signal:=9} caslock_;
    pkill -${signal:=9} chtlock_;
    pkill -${signal:=9} cqlock_;
    pkill -${signal:=9} benchmark;
    pkill -${signal:=9} coordinator;
    killall -${signal:=9} client 2>/dev/null;
    killall -${signal:=9} server 2>/dev/null;
  " &
  i=`expr $i + 1`
done

# release_lock