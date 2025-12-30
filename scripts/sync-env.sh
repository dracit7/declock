#!/bin/bash

echo "Project root: ${DMLOCK_PATH:?'

  The project root is not set. Please run `source scripts/set-env.sh` 
  before executing this script.
'}"

targets=`echo $HOSTS | tr ' ' '\n' | awk -F'-' '{print $1}'`
targets=$(echo "$targets" | tr ' ' '\n' | sort -u | tr '\n' ' ')

for host in $targets; do
  rsync -a $DMLOCK_PATH/scripts/set-env.sh $host:~/
  ssh $host "mkdir -p $DMLOCK_PATH $DMLOCK_LOG_PATH"

  echo "syncing to $host..."
  rsync -a $DMLOCK_PATH/build $host:$DMLOCK_PATH/ &
  rsync -a $DMLOCK_PATH/scripts $host:$DMLOCK_PATH/ &
  rsync -a $DMLOCK_PATH/traces $host:$DMLOCK_PATH/ &
done

wait