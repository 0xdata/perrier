#!/usr/bin/env bash

PREFIX=org.apache.spark.examples.h2o
DEFAULT_EXAMPLE=ProstateDemo

if [ $1 ]; then
  EXAMPLE=$PREFIX.$1
else
  EXAMPLE=$PREFIX.$DEFAULT_EXAMPLE
fi

EXAMPLE_MASTER=${MASTER:-"local[*]"}
EXAMPLE_DEPLOY_MODE="cluster"
EXAMPLE_DEPLOY_MODE=${DEPLOY_MODE:-"client"} 
EXAMPLE_NUM_OF_H2O_WORKERS=${NUM_WORKERS:-3} # 2 real workers + 1 in driver
#./make-package.sh

echo "---------"
echo "  Using example: $EXAMPLE"
echo "  Using master : $EXAMPLE_MASTER"
echo "  Deploy mode  : $EXAMPLE_DEPLOY_MODE"
echo "---------"
export SPARK_PRINT_LAUNCH_COMMAND=1
VERBOSE=--verbose
VERBOSE=
( cd ../; bin/spark-submit $VERBOSE --driver-memory 3G --driver-java-options "-Dspark.h2o.workers=$EXAMPLE_NUM_OF_H2O_WORKERS" --master $EXAMPLE_MASTER --deploy-mode $EXAMPLE_DEPLOY_MODE --class $EXAMPLE h2o-examples/target/shaded.jar )
