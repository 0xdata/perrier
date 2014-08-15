package org.apache.spark.h2o

import scala.language.implicitConversions
import org.apache.spark.{TaskContext, SparkContext}
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.rdd.{H2ORDD, RDD}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

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

  private def perPartition[A]( h2ordd: H2ORDD[A] ) ( context: TaskContext, it: Iterator[A] ): Int = {
    println(context.partitionId+", name: "+h2ordd.h2oName)
    it.next()
    val f = it.next()

    context.partitionId
  }

  // Implicit conversion creating hiding H2O like RDD.
  // Needs import: import scala.language.implicitConversions
  def createH2ORDD[A: TypeTag: ClassTag](rdd: RDD[A], name: String): H2ORDD[A] = {
    val tt = typeOf[A].members.filter(!_.isMethod)
    val ts = tt.foreach(sym => println(sym+" "+sym.typeSignature))

    val h2ordd = new H2ORDD(name, this, this.sparkContext, rdd)
    sparkContext.runJob(h2ordd, perPartition[A](h2ordd) _) // eager, not lazy, evaluation
    //h2ordd.fr.finizalizePartialFrame();
    h2ordd
  }
}
