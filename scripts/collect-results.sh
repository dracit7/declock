#!/bin/bash

source ~/set-env.sh

hosts=(`echo $HOSTS | tr ' ' '\n' | awk -F'-' '{print $1}' | uniq`)
usage="Usage: collect-lat.sh [test] [#cns] [out_dir]"

test=${1:?$usage}
cn_num=${2:?$usage}
test_name=${3:?$usage}
mn_num=${4:?$usage}

out_dir="$RESULT_PATH/$test_name"
mkdir -p $out_dir
out_file="$out_dir/latency-raw"
rm -f $out_file

case $test in
  *microbench)
    for ((i=${mn_num}; i<${cn_num}+${mn_num}; i++)); do
      log_file="$test-h$i"
      log_file_path="$DMLOCK_LOG_PATH/$log_file"
      while true; do
        scp $USERNAME@${hosts[i]}:${log_file_path} $out_dir/
        if [[ $? == 0 ]]; then break; fi
      done

      total_lines=$(wc -l < $out_dir/$log_file)
      without_tail_lines=$((total_lines - 2))
      without_head_lines=$((total_lines - 4))
      head -n $without_tail_lines $out_dir/$log_file | \
        tail -n $without_head_lines >>$out_file
      rm $out_dir/$log_file
    done

    grep ",0$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-acq
    grep ",1$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-rel
    grep ",2$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-reset
    grep ",12$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-data
    grep ",13$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-op
    grep ",17$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-acq-0
    # grep ",9$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-break1
    # grep ",10$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-break2
    # grep ",11$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-break3

    ./scripts/calculator.sh tmp "request throughput" > $out_dir/thpt
    ./scripts/calculator.sh tmp "rdma read throughput" > $out_dir/thpt-read
    ./scripts/calculator.sh tmp "rdma write throughput" > $out_dir/thpt-write
    ./scripts/calculator.sh tmp "rdma cas throughput" > $out_dir/thpt-cas
    ./scripts/calculator.sh tmp "rdma fa throughput" > $out_dir/thpt-faa
    ./scripts/calculator.sh tmp "rdma.*throughput" > $out_dir/thpt-rdma
    ./scripts/calculator.sh tmp "lqe fetch freq" > $out_dir/fetch-freq
    ./scripts/calculator.sh tmp "lock reset cnt" > $out_dir/reset-cnt
  ;;

  shiftlock|shiftlock-txn)
    ./scripts/calculator.sh tmp "request throughput" > $out_dir/thpt
    ./scripts/calculator.sh tmp "per acquire:" > $out_dir/nops-per-acq

    for ((i=0; i<${cn_num}; i++)); do
      log_file="output-$((i % 2))"
      log_file_path="shiftlock/results/$log_file"
      while true; do
        j=$((i / 2 + 1))
        scp $USERNAME@${hosts[j]}:${log_file_path} $out_dir/
        if [[ $? == 0 ]]; then break; fi
      done

      cat $out_dir/$log_file >>$out_file
      rm $out_dir/$log_file
    done

    grep ",0$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-op
    grep ",1$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-acq
    grep ",2$" $out_file | awk -F, '{ print $1 }' > $out_dir/latency-acq-0
  ;;
esac
