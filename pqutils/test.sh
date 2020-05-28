#!/usr/bin/bash

function parquet() {
  /usr/local/hadoop/bin/hadoop jar /home/sme/src/parquet-mr/parquet-cli/target/parquet-cli-1.12.0-SNAPSHOT-runtime.jar org.apache.parquet.cli.Main --dollar-zero paruqet "$@"
}

RET=1
while [ $RET -eq "1" ]; do
  echo "Trying to connect to scylla"
  echo exit | cqlsh
  RET=$?
  if [ $RET -ne "0" ]; then
    sleep 2
  fi
done
echo "Connected to Scylla"

otificrm -rf /tmp/scylla-parquet/my_table*
cat $1 | cqlsh
JAVA_HOME="/home/sme/.sdkman/candidates/java/8.0.252.hs-adpt" nodetool flush mk && \
JAVA_HOME="/home/sme/.sdkman/candidates/java/8.0.252.hs-adpt" nodetool compact mk

echo "DONE"
