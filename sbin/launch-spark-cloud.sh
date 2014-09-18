export SPARK_PRINT_LAUNCH_COMMAND=1
export SPARK_MASTER_IP="localhost"
export SPARK_MASTER_PORT="7077"
export SPARK_WORKER_PORT="7087"
export SPARK_WORKER_INSTANCES=2
export MASTER="spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"

# cleanup
rm -rf ../work/*
rm -rf ../logs/*



echo "Starting master..."
./start-master.sh 

NUM_WORKERS=${1:-2}
echo "Starting $NUM_WORKERS workers..."
for N in $(seq 1 $NUM_WORKERS); do
  ./start-slave.sh $N $MASTER
done

# launch spark shell
#../bin/spark-shell

