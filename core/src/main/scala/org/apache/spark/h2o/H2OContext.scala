package org.apache.spark.h2o

import scala.language.implicitConversions
import org.apache.spark.SparkContext
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.rdd.{H2ORDD, RDD}

import scala.reflect.runtime.universe.TypeTag

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

  /** Register RDD inside H2O. */
  def registerRDDAsFrame(h2oRDD: H2ORDD, frameName: String): Unit = {
    println( s"Registering $h2oRDD as frame $frameName")
  }

  // Implicit conversion creating hiding H2O like RDD.
  // Needs import: import scala.language.implicitConversions
  implicit def createH2ORDD[A <: Product: TypeTag](rdd: RDD[A]): H2ORDD = {
    // Dummy get type name
    val tt = implicitly[TypeTag[A]]
    println(s"implicit conversion createH2ORDD[$tt](rdd=$rdd)")
    new H2ORDD(this, this.sparkContext)
  }
}
