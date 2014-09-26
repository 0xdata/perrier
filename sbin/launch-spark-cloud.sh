export SPARK_PRINT_LAUNCH_COMMAND=1
export SPARK_MASTER_IP="localhost"
export SPARK_MASTER_PORT="7077"
export SPARK_WORKER_PORT="7087"
export SPARK_WORKER_INSTANCES=2
export MASTER="spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"

# Number of workers
NUM_WORKERS=${1:-3}
# Spark dir
TDIR=$(cd `dirname $0` &&  pwd)
# Configure tmp dir
tmpdir=${TMPDIR:-"/tmp/"}
export SPARK_LOG_DIR="${tmpdir}spark/logs"
export SPARK_WORKER_DIR="${tmpdir}spark/work"
export SPARK_LOCAL_DIRS="${tmpdir}spark/work"

# Cleanup
rm -rf $TDIR/work/* 2>/dev/null
rm -rf $TDIR/logs/* 2>/dev/null

cat <<EOF
Starting Spark cluster ... 1 master + $NUM_WORKERS workers
 * Log dir is located in $SPARK_LOG_DIR"
 * Workers dir is located in $SPARK_WORKER_DIR"
EOF

echo "Starting master..."
$TDIR/start-master.sh 

echo "Starting $NUM_WORKERS workers..."
for N in $(seq 1 $NUM_WORKERS); do
  $TDIR/start-slave.sh $N $MASTER
done

# launch spark shell
#../bin/spark-shell

