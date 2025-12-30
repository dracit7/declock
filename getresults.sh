
# test=wo-cohort
test=$1
system=$2
function=$3

PROJ_ROOT=$(dirname "${0}")
source $PROJ_ROOT/scripts/set-env.sh

if [ $system == 'shiftlock' ]; then
  bench=$system
else
  bench=${system}_microbench
fi

dist=0.99 
cn_num=8 
mn_num=1
rd_ratio=50
core_num=16
mgc=1
locality=0
lock_num=100000
exec_time=1
datasize=8
crt_num=2
qcap=0

if [[ $function == 'data' || $function == 'all' ]]; then
  if [[ $test != 'store' && $test != 'txn' ]]; then
    rm -rf $REPORT_PATH/$test/$system-*
  fi
  mkdir -p $REPORT_PATH/$test
fi

run_tests() {
  if [[ $function == "run" || $function == 'all' ]]; then
    echo "Running test $1"
    terminate="no"
    while [ $terminate == "no" ]; do
      ssh $MASTER_NODE "$DMLOCK_PATH/scripts/run-on-all.sh \
        $bench $dist $rd_ratio $cn_num $core_num \
        $crt_num $lock_num $locality $datasize $exec_time \
        $mgc $qcap $mn_num >tmp 2>&1 &"
      SECONDS=0
      while [ $SECONDS -lt 40 ]; do
        sleep 1;
        scp $MASTER_NODE:tmp ./ >/dev/null
        exit_cnt=$(grep -c "exited" tmp)
        if [[ $exit_cnt == $cn_num ]]; then 
          terminate="yes"
          break; 
        fi
        if grep "Connection closed" tmp; then break; fi
      done
      ssh $MASTER_NODE bash $DMLOCK_PATH/scripts/kill-all.sh
      sleep 4
    done
    ./scripts/collect-results.sh $bench $cn_num $1 $mn_num
  fi
}

run_tests_failure() {
  ./scripts/run-on-all.sh $bench $dist $rd_ratio $cn_num $core_num \
    $crt_num $locality $datasize $exec_time $mgc $qcap 2>tmp
  while true; do
    if grep "periodic" tmp >/dev/null; then break; fi
  done
  sleep 1;
  bash scripts/simulate-failure.sh
  sleep 2;
  ./scripts/kill-all.sh
}

report_data() {
  if [[ $function == 'data' || $function == 'all' ]]; then
    thpt=`cat data/$2/thpt`
    # lat_acq=`python3 scripts/results/get-percent.py $2 acq 2>/dev/null`
    # lat_data=`python3 scripts/results/get-percent.py $2 data 2>/dev/null`
    lat_op=`python3 scripts/results/get-percent.py $2 op`
    echo "thpt: $thpt"
    echo $lat_op
    # acq_p99=`echo $lat_acq | awk '{print $6}'`
    # acq_p50=`echo $lat_acq | awk '{print $2}'`
    # data_p50=`echo $lat_data | awk '{print $2}'`
    op_p99=`echo $lat_op | awk '{print $6}'`
    op_p50=`echo $lat_op | awk '{print $2}'`

    if [[ $test == 'store' ]]; then
      echo "$1 $thpt $op_p99 $op_p50" >> \
        $REPORT_PATH/$test/$system-$dist-$rd_ratio-$datasize
    elif [[ $test == 'txn' ]]; then
      echo "$1 $thpt $op_p99 $op_p50" >> \
        $REPORT_PATH/$test/$system-$rd_ratio 
    else
      echo "$1 $thpt" >> $REPORT_PATH/$test/$system-thpt
      echo "$1 $op_p99" >> $REPORT_PATH/$test/$system-lat99-op
      echo "$1 $op_p50" >> $REPORT_PATH/$test/$system-lat50-op
    fi
    # echo "$cnum $acq_p99" >> $REPORT_PATH/$test/$system-p$rw-lat99
    # echo "$cnum $acq_p50" >> $REPORT_PATH/$test/$system-p$rw-lat50
    # echo "$cnum $data_p50" >> $REPORT_PATH/$test/$system-p$rw-latdata
  fi
}

run_sherman() {
  ./scripts/run-on-all.sh $test $cn_num $rd_ratio $core_num $system
    $crt_num >tmp
  while true; do
    sleep 1;
    has_thpt=`grep -c "cluster throughput" tmp`
    has_lat=`grep -c "p50" tmp`
    if [[ $has_thpt && $has_lat ]]; then break; fi
  done
  ./scripts/kill-all.sh
}

collect_sherman() {
  mkdir -p $outf-$test
  grep "cluster throughput" tmp | \
    tail -n 1 | awk '{ print $3 }' > $outf-$test/thpt
  grep "p50" tmp | tail -n 1 | awk '{ print $2 }' > $outf-$test/lat50
  grep "p50" tmp | tail -n 1 | awk '{ print $8 }' > $outf-$test/lat99
}

count_rdma_per_acq() {
  thpt_req=`cat data/$1/thpt`
  thpt_cas=`cat data/$1/thpt-cas`
  thpt_faa=`cat data/$1/thpt-faa`
  thpt_rd=`cat data/$1/thpt-read`
  thpt_wr=`cat data/$1/thpt-write`
  thpt_rdma=`cat data/$1/thpt-rdma`
  fetch=`cat data/$1/fetch-freq`

  rdbias=$(echo "$thpt_req * $rd_ratio / 100 * $exec_time" | bc -l)
  wrbias=$(echo "$thpt_req * $exec_time - $rdbias" | bc -l)

  case $system in 
    caslock)
      thpt_rdlock=$(echo "$thpt_faa / 2 + $thpt_rd - $rdbias" | bc -l)
      thpt_wrlock=$(echo "$thpt_cas" | bc -l)
      thpt_lock=$(echo "$thpt_rdlock + $thpt_wrlock" | bc -l)
    ;;
    dmlock|cqlock|dmlock_tf)
      thpt_lock=$(echo "$thpt_faa / 2 + $thpt_wr - $wrbias" | bc -l)
    ;;
    dslr)
      thpt_lock=$(echo "$thpt_faa / 2 + $thpt_rd - $rdbias" | bc -l)
    ;;
  esac

  rdmaperlock=$(echo "$thpt_lock / $thpt_req" | bc -l)
  retry_ratio=$(echo "$thpt_lock / $thpt_rdma" | bc -l)
  fetch_freq=$(echo "$fetch / $cn_num / $core_num " | bc -l)
  # echo "$i $fetch_freq" >> $REPORT_PATH/$test/$system-fetch
  # echo "$i $retry_ratio" >> $REPORT_PATH/$test/$system-retry
}

report_data_app() {
  thpt=`cat data/data/$outf-$test/thpt`
  lat_op=`python3 scripts/results/get-percent.py $outf-$test op 2>/dev/null`
  echo $lat_op

  op_p99=`echo $lat_op | awk '{print $6}'`
  op_p50=`echo $lat_op | awk '{print $2}'`
  cnum=$(($cn_num * $core_num * $crt_num))
  echo "$cnum $thpt $op_p99 $op_p50" >> $REPORT_PATH/$test/$system
}

case $test in
  intra-cn)
    corerange=${4:-"1 2 4 6 8 10 12 14 16"}
    cn_num=1
    for i in `echo $corerange`; do
      testcase=$test-$system-${i}core
      core_num=$i
      run_tests $testcase
      report_data $i $testcase
    done
  ;;

  inter-cn)
    cnrange=${4:-"1 2 3 4 5 6 7 8 10 12 14 16 20 24 28 31"}
    for i in `echo $cnrange`; do
      testcase=$test-$system-${i}cn
      cn_num=$i
      run_tests $testcase
      report_data $i $testcase
    done
  ;;

  read-ratio)
    rwrange=${4:-"0 10 20 30 40 50 60 70 80 90 100"}
    for i in `echo $rwrange`; do
      testcase=$test-$system-${i}rd
      rd_ratio=$i
      run_tests $testcase
      report_data $i $testcase
    done
  ;;

  lock-num)
    range=${4:-"1000 10000 100000 1000000 10000000"}
    for i in `echo $range`; do
      testcase=$test-$system-${i}locks
      lock_num=$i
      run_tests $testcase
      report_data $i $testcase
    done
  ;;

  zipf-alpha)
    range=${4:-"0.999 0.99 0.9 0.5 0.0"}
    i=0
    for alpha in `echo $range`; do
      testcase=$test-$system-zipf${alpha}
      dist=$alpha
      run_tests $testcase
      report_data $i $testcase
      i=$((i + 1))
    done
  ;;

  locality)
    mgc=${4:-"4"}
    range=${5:-"0 20 40 60 80 100"}
    for i in `echo $range`; do
      if [ $system == 'chtlock' ]; 
      then testcase=$test-$system-$mgc-${i}local;
      else testcase=$test-$system-${i}local; fi
      locality=$i
      run_tests $testcase
      report_data $i $testcase
    done
  ;;

  exec-time)
    range=${4:-"0 1 2 4 8 16 32"}
    for i in `echo $range`; do
      testcase=$test-$system-${i}exec
      exec_time=$i
      run_tests $testcase
      if [[ $i == "0" ]]; then i=0.5; fi
      report_data $i $testcase
    done
  ;;

  cdf)
    mgc=${4:-"4"}
    rd_ratio=${5:-"50"}
    if [ $system == 'chtlock' ]; 
    then testcase=$test-$system-$mgc-rw$rd_ratio;
    else testcase=$test-$system-rw$rd_ratio; fi

    run_tests $testcase
    python3 ./scripts/results/gen-cdf-data.py $testcase \
      >$REPORT_PATH/cdf/$testcase
  ;;

  client-num)
    corerange=${4:-"1 2 4 6 8 10 12 14 16"}
    cnrange=${5:-"2 3 4 5 6 7 8"}
    cn_num=1
    for i in `echo $corerange`; do
      testcase=$test-$system-$((i * 2))clients
      core_num=$i
      run_tests $testcase
      report_data $((i * 2)) $testcase
    done
    for i in `echo $cnrange`; do
      testcase=$test-$system-$((i * 32))clients
      cn_num=$i
      run_tests $testcase
      report_data $((i * 32)) $testcase
    done
  ;;

  rops)
    range=${4:-"0 1 2 4 8 16 32"}
    rm -f $REPORT_PATH/$test/$system
    for i in `echo $range`; do
      if [ $system == 'shiftlock' ]; then
        nops=$(cat data/exec-time-$system-${i}exec/nops-per-acq)
        rdmaperlock=$(echo "$nops / $cn_num / $core_num / $crt_num" | bc -l)
      else
        exec_time=$i
        count_rdma_per_acq exec-time-$system-${i}exec
      fi
      if [[ $i == "0" ]]; then i=0.5; fi
      echo "$i $rdmaperlock" >> $REPORT_PATH/$test/$system
    done
  ;;

  fetch)
    rwrange=${4:-"0 50 90"}
    ecrange=${5:-"0 1 2 4 8 16"}
    for rw in `echo $rwrange`; do
      rd_ratio=$rw
      rm -f $REPORT_PATH/$test/rw$rw
      for i in `echo $ecrange`; do
        exec_time=$i
        testcase=$test-ec$i-rw$rw
        run_tests $testcase
        count_rdma_per_acq $testcase
        if [[ $i == "0" ]]; then i=0.5; fi
        echo "$i $fetch_freq" >> $REPORT_PATH/$test/rw$rw
      done
    done
  ;;

  qcap)
    dsrange=${4:-"0.0 0.9 0.99"}
    qcrange=${5:-"8 16 32 64 128 256 512 1024 2048"}
    for ds in `echo $dsrange`; do
      dist=$ds
      rm -f $REPORT_PATH/$test/ds$ds
      for i in `echo $qcrange`; do
        qcap=$i
        testcase=$test-zipf$ds-qcap$i
        run_tests $testcase
        lat=`python3 scripts/results/get-percent.py $testcase rel 2>/dev/null`
        p50=`echo $lat | awk '{print $2}'`
        echo "$i $p50" >> $REPORT_PATH/$test/zipf$ds
      done
    done
  ;;

  store)
    dist=${4:?"missing argument"}
    rd_ratio=${5:?"missing argument"}
    datasize=${6:?"missing argument"}
    cnrange=${7:-"2 3 4 5 6 7 8"}
    corerange=${8:-"1 2 4 6 8 10 12 14 16"}

    cn_num=1
    for i in `echo $corerange`; do
      testcase=$test-$system-$((i * 2))clients
      core_num=$i
      run_tests $testcase
      report_data $((i * 2)) $testcase
    done

    for i in `echo $cnrange`; do
      testcase=$test-$system-$((i * 32))clients
      cn_num=$i
      run_tests $testcase
      report_data $((i * 32)) $testcase
    done
  ;;

  txn)
    workload=${4:?"missing argument"}
    cnrange=${5:-"2 3 4 5 6 7 8"}
    corerange=${6:-"1 2 4 6 8 10 12 14 16"}

    if [ $system == "shiftlock" ]; then
      dist=$workload
      bench="shiftlock-txn"
    else
      dist="txn"
      rd_ratio=$workload
    fi
    cn_num=1
    for i in `echo $corerange`; do
      testcase=$test-$system-$((i * 2))clients
      core_num=$i
      run_tests $testcase
      report_data $((i * 2)) $testcase
    done

    for i in `echo $cnrange`; do
      testcase=$test-$system-$((i * 32))clients
      cn_num=$i
      run_tests $testcase
      report_data $((i * 32)) $testcase
    done
  ;;

  reset)
    cnrange=${4:-"1 2 3 4 5 6 7 8"}
    update_crt 2
    update_rw 0
    for i in `echo $cnrange`; do
      update_cns $i
      qcap=$(($cn_num * $core_num * $crt_num - 1))
      echo "Running test $outf-$test-qcap$qcap"
      run_tests
      collect_results
    done
  ;;

  sherman)
    rws=${4:-"0 50 95"}
    update_crt 2
    for rw in $rws; do
      update_rw $rw
      echo "Running test $outf-$test"
      run_sherman
      collect_sherman
    done
  ;;

  mn-num)
    mnrange=${4:-"1 2 4 8"}
    for i in `echo $mnrange`; do
      testcase=$test-$system-${i}mn
      mn_num=$i
      run_tests $testcase
      report_data $i $testcase
    done
  ;;

  failure)
    core_num=${4:-"16"}
    update_rw 0
    update_cns 8
    update_cores $core_num
    update_crt 2
    run_tests_failure
    bash scripts/results/periodic-thpt.sh > $REPORT_PATH/$test/cn-failure-$core_num
  ;;

esac
