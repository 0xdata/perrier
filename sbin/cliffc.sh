#!/bin/bash
export SPARK_MASTER_IP="localhost"
export SPARK_MASTER_PORT="7077"
export SPARK_WORKER_PORT="7087"
export SPARK_WORKER_INSTANCES=1
export MASTER="spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
echo .
echo .
echo ============================
echo .   MASTER
echo .
./start-master.sh 
echo .
echo .
echo ============================
echo .   WORKER
echo .
./start-slave.sh 1 $MASTER

# launch spark shell
echo .
echo .
echo ============================
echo .   SHELL
echo .
../bin/spark-shell

