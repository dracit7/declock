
LOCK_FILE='/tmp/dm.lock'

function try_lock {

  while [ -f $LOCK_FILE ]; do
    echo "$LOCK_FILE exists! Waiting..."
    sleep 5
  done
  touch $LOCK_FILE
  echo "$LOCK_FILE created"
}

function release_lock {
  if [ -f $LOCK_FILE ]; then
    rm $LOCK_FILE
    echo "$LOCK_FILE removed"
  else
    echo "Fatal!!! Cannot find $LOCK_FILE when releasing lock."
    exit -1
  fi
}

