#!/bin/bash

source ~/set-env.sh

mid=$1
numa_id=$2
test=$3
binary=$4
cn_num=$5
mn_num=$6
core_num=$7
crt_num=$8
cap=$9

out_file=$DMLOCK_LOG_PATH/$test-h$mid
machine_num=$(expr $cn_num + $mn_num)

case $test in
  test_rdma_*)
    out_file=/dev/null
  ;;
esac

all_ips=($HOSTS)

for ((i=0; i<$machine_num; i++)); do
  if ((i < $mn_num)); then nt='mn'; else nt='cn'; fi
  ip=`echo ${all_ips[i]} | tr ' ' '\n' | awk -F'-' '{print $2}'`
  ips="$ips;$nt-$ip"
done
ips=`echo $ips | sed 's/^;//'`

MACHINE_ID=$mid \
NUMA_ID=$numa_id \
MACHINE_IPS="$ips" \
QUEUE_CAPACITY="$cap" \
CORE_NUM="$core_num" \
COROUTINE_NUM="$crt_num" \
numactl --cpubind=$numa_id --membind=$numa_id \
$DMLOCK_PATH/build/$binary ${@:10} \
>$out_file