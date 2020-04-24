#!/usr/bin/bash

ulimit -n 65535
mkdir -p /tmp/scylla-parquet
SCYLLA_PID=""

function sync_kill() {
  while kill $1; do
    sleep 1
  done
}

trap "([ -z "$SCYLLA_PID" ] && echo || sync_kill SCYLLA_PID) && echo Shutting down && exit" SIGINT SIGTERM

while true; do
  echo "RUNNING SCYLLA UNDER PID $SCYLLA_PID"
  echo "PRESS ANY KEY TO REBUILD..."

  read ACTION

  # normal restart
  if [ "$ACTION" == "r" ]; then
    ([ -z "$SCYLLA_PID" ] && echo || sync_kill $SCYLLA_PID)
  # if scylla is not responding to sigterm send sigkill
  elif [ "$ACTION" == "f" ]; then
    rm -rf ~/scylla/datadir
    rm -rf /tmp/scylla-parquet/*
    ([ -z "$SCYLLA_PID" ] && echo || kill -9 $(pgrep scylla-dev))
  else
    echo "Enter r for SIGTERM or f for SIGKILL and then rebuild"
    continue
  fi
  SCYLLA_PID=""
  echo "****************************************************************************************************************"
  echo "********************************** STARTING BUILD **************************************************************"
  echo "****************************************************************************************************************"

  PKG_CONFIG_PATH=/home/sme/scylla/scylla-local PATH=/usr/lib/ccache:$PATH ninja -j12 build/dev/scylla
  BUILD_STATUS=$?
  notify-send "Build finished with status ${BUILD_STATUS}"

  echo "________________________________________________________________________________________________________________"
  echo "__________________________________ FINISHED BUILD ______________________________________________________________"
  echo "________________________________________________________________________________________________________________"

  if [ "$BUILD_STATUS" -eq "0" ]; then
    scylla-dev --workdir /home/sme/scylla/datadir --smp 1 --max-io-requests 10 &
    SCYLLA_PID=$!
  fi

done
