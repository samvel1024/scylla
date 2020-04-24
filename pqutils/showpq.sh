#!/usr/bin/bash

PKG_CONFIG_PATH=/home/sme/scylla/scylla-local PATH=/usr/lib/ccache:$PATH ninja -j12 build/dev/experiment ; notify-send "Build finished with status $?"
echo "Removing previous file"
rm /tmp/pq-test.parquet
echo "Running test"
./build/dev/experiment
