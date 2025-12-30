#!/bin/bash

script_dir=$(dirname "${0}")
source $script_dir/set-env.sh
source $script_dir/utils.sh

# Program starting from here
# try_lock

test=${1:?"Usage: run-on-all.sh [test_name]"}

script="bash $DMLOCK_PATH/scripts/run-instance.sh"
hosts=(`echo $HOSTS | tr ' ' '\n' | awk -F'-' '{print $1}'`)
ips=(`echo $HOSTS | tr ' ' '\n' | awk -F'-' '{print $2}'`)
numa=(`echo $HOSTS | tr ' ' '\n' | awk -F'-' '{print $3}'`)

case $test in
  sherman)
    usage="Usage: sherman [cn_num] [read_ratio] [thread_count] [basic/locallock/dmlock] [coro_count=1]"
    cn_num=${2:?"$usage"}
    read_ratio=${3:?"$usage"}
    thread_count=${4:?"$usage"}
    lock_system=${5:?"$usage"}
    coro_count=${6:-1}
    mn_num=${7:-1}

    if [ "${lock_system}" != "basic" ] && [ "${lock_system}" != "locallock" ] && [ "${lock_system}" != "dmlock" ]; then
      echo "Invalid lock system name ${lock_system}" >&2
      release_lock
      exit 1
    fi


    script="bash ${SHERMAN_PATH}/script/run-instance.sh"
    binary_path="${SHERMAN_PATH}/build/benchmark_${lock_system}"

    node_count=$(expr $cn_num + $mn_num)
    args="${node_count} ${read_ratio} ${thread_count} ${coro_count} ${mn_num}"
    ${SHERMAN_PATH}/script/restartMemc.sh
    sleep 5

    for ((i=0; i<${node_count}; i++)) do
      node_i=$((i*2))
      echo "$USERNAME@${hosts[node_i]}"
      ssh $USERNAME@${hosts[node_i]} "${script} $i ${numa[node_i]} ${binary_path} ${args}" &
    done
  ;;

  *microbench)
    usage="Usage: microbench [uniform|zipfian] [read_ratio] [cn_num] [cn_core_num] [max_grant_cnt] [batch_num=1]"
    req_dist=${2:?$usage}
    rw_ratio=${3:?$usage}
    cn_num=${4:?$usage}
    cn_core_num=${5:?$usage}
    coroutine_num=${6:-1}
    lock_num=${7:-100000}
    locality=${8:-0}
    data_size=${9:-8}
    exec_time=${10:-1}
    grant_cnt=${11:-1}
    lq_cap=${12:-0}
    mn_num=${13:-1}

    binary=$test
    machine_num=$(expr $cn_num + $mn_num)
    command="$test $binary $cn_num $mn_num $cn_core_num $coroutine_num $lq_cap"
    args="$req_dist $rw_ratio $exec_time $locality $grant_cnt $data_size $lock_num"

    ssh ${hosts[0]} "$DMLOCK_PATH/build/coordinator" &
    for ((i=0; i<$mn_num; i++)); do
      ssh ${hosts[i]} "$script $i ${numa[i]} $command mn $args" &
    done
    for ((i=$mn_num; i<$machine_num; i++)); do
      ssh ${hosts[i]} "$script $i ${numa[i]} $command cn $args" &
    done
  ;;

  shiftlock)
    usage="Usage: microbench [zipf alpha] [read_ratio] [cn_num] [cn_core_num]"
    req_dist=${2:?$usage}
    rw_ratio=${3:?$usage}
    cn_num=${4:?$usage}
    cn_core_num=${5:?$usage}
    coroutine_num=${6:-1}
    lock_num=${7:-100000}
    locality=${8:-0}
    data_size=${9:-8}
    exec_time=${10:-1}
    mn_num=${13:-1}

    machine_num=$(expr $cn_num + $mn_num)
    rw_float=$(awk "BEGIN {printf \"%.2f\", $rw_ratio/100}")
    test_time=2
    if [ $lock_num == 10000000 ]; then test_time=4; fi

    ssh ${hosts[0]} "bash shiftlock/scripts/run-basic.sh ShiftLock micro:$req_dist,$rw_float:$test_time $cn_num $cn_core_num $lock_num $locality $exec_time $data_size $mn_num" &
  ;;

  shiftlock-txn)
    usage="Usage: microbench [zipf alpha] [read_ratio] [cn_num] [cn_core_num]"
    trace=${2:?$usage}
    rw_ratio=${3:?$usage}
    cn_num=${4:?$usage}
    cn_core_num=${5:?$usage}

    ssh ${hosts[0]} "bash shiftlock/scripts/run-basic.sh ShiftLock trace:$trace:2 $cn_num $cn_core_num" &
  ;;


  *)
    echo "Unsupported test $test. Please check the test name."
  ;;
esac
