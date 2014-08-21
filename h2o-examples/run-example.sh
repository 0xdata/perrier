#!/usr/bin/env bash

PREFIX=org.apache.spark.examples.h2o
DEFAULT_EXAMPLE=ProstateDemo

if [ $1 ]; then
  EXAMPLE=$PREFIX.$1
else
  EXAMPLE=$PREFIX.$DEFAULT_EXAMPLE
fi

EXAMPLE_MASTER=${MASTER:-"local[*]"}

#./make-package.sh

echo "---------"
echo "  Using master : $EXAMPLE_MASTER"
echo "  Using example: $EXAMPLE"
echo "---------"

( cd ../; bin/spark-submit --verbose --master $EXAMPLE_MASTER --class $EXAMPLE h2o-examples/target/shaded.jar )
