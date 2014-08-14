package org.apache.spark.h2o

import scala.language.implicitConversions
import org.apache.spark.SparkContext
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.rdd.{H2ORDD, RDD}
import scala.reflect.ClassTag

/**
 * Simple H2O context motivated by SQLContext.
 *
 * Doing - implicit conversion from RDD -> H2OLikeRDD
 */
@AlphaComponent
class H2OContext(@transient val sparkContext: SparkContext)
  extends org.apache.spark.Logging
  with H2OConf
  with Serializable {

  private def perPartition[T]( idx: Int, it: Iterator[T] ): Iterator[T] = {
    println(idx+" size:" + it.size)
    it
  }

  // Implicit conversion creating hiding H2O like RDD.
  // Needs import: import scala.language.implicitConversions
  def createH2ORDD[A: ClassTag](rdd: RDD[A], name: String): H2ORDD[A] = {
    val h2ordd = new H2ORDD(this, this.sparkContext, rdd)
    val x = h2ordd.mapPartitionsWithIndex(perPartition[A], preservesPartitioning=true)
    x.count()
    h2ordd
  }
}
