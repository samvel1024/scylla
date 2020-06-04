#!/usr/bin/bash

ulimit -n 65535
sudo chmod 1777 /tmp
mkdir -p /tmp/scylla-parquet
sudo ln -s ${HOME}/parquet4seastar/include/parquet4seastar /usr/include
SCYLLA_PID=""

function sync_kill() {
  while kill $1; do
    sleep 1
  done
}

bash -c "cd /scylla-jmx && ./scripts/scylla-jmx" &
echo Started JMX

export LIBRARY_PATH=${HOME}/parquet4seastar/build/:/thrift/lib/cpp/.libs/:${LIBRARY_PATH}
export LD_LIBRARY_PATH=${HOME}/parquet4seastar/build/:/thrift/lib/cpp/.libs/:${LD_LIBRARY_PATH}
trap 'jobs -p && kill $(jobs -p)' EXIT


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

  PKG_CONFIG_PATH=${HOME}/scylla/scylla-local PATH=/usr/lib64/ccache:$PATH ninja -j6 build/dev/scylla
  BUILD_STATUS=$?
#  notify-send "Build finished with status ${BUILD_STATUS}"

  echo "________________________________________________________________________________________________________________"
  echo "__________________________________ FINISHED BUILD ______________________________________________________________"
  echo "________________________________________________________________________________________________________________"

  if [ "$BUILD_STATUS" -eq "0" ]; then
    ./build/dev/scylla --workdir ${HOME}/scylla/datadir --max-io-requests 10 &
    SCYLLA_PID=$!
  fi

done
