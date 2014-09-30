package org.apache.spark

/** Type shortcuts to simplify work in Sparkling REPL */
package object h2o {
  type Frame = water.fvec.Frame
  type Key = water.Key
  type H2O = water.H2O
  type DataFrame = water.fvec.DataFrame
  type RDD[X] = org.apache.spark.rdd.RDD[X]
}
