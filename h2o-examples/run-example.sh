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

#./make-package.sh

echo "---------"
echo "  Using example: $EXAMPLE"
echo "  Using master : $EXAMPLE_MASTER"
echo "  Deploy mode  : $EXAMPLE_DEPLOY_MODE"
echo "---------"
export SPARK_PRINT_LAUNCH_COMMAND=1
( cd ../; bin/spark-submit --verbose --master $EXAMPLE_MASTER --deploy-mode $EXAMPLE_DEPLOY_MODE --class $EXAMPLE h2o-examples/target/shaded.jar )
