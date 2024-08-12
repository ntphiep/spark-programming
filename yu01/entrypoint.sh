#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
    start-master.sh -p 7077
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
    start-worker.sh spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ]
then
    start-history-server.sh
elif [ "$SPARK_WORKLOAD" == "connect" ]
then    
    start-connect-server.sh --master spark://spark-master:7077 --packages org.apache.spark:spark-connect_2.12:3.5.1
# else
    # echo "Invalid SPARK_WORKLOAD"
fi