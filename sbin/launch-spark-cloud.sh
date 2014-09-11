export SPARK_PRINT_LAUNCH_COMMAND=1
export SPARK_MASTER_IP="localhost"
export SPARK_MASTER_PORT="7077"
export SPARK_WORKER_PORT="7087"
export SPARK_WORKER_INSTANCES=2
export MASTER="spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"

# cleanup
rm -rf ../work/*
rm -rf ../logs/*

./start-master.sh 
./start-slave.sh 1 $MASTER
./start-slave.sh 2 $MASTER

# launch spark shell
#../bin/spark-shell

