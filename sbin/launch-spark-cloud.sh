export SPARK_PRINT_LAUNCH_COMMAND=1
export SPARK_MASTER_IP="localhost"
export SPARK_MASTER_PORT="7077"
export SPARK_WORKER_PORT="7087"
export SPARK_WORKER_INSTANCES=1
export MASTER="spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
./start-master.sh 
./start-slave.sh 1 $MASTER

# launch spark shell
#../bin/spark-shell

