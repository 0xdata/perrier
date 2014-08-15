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

  private def perPartition[A: ClassTag]( h2ordd: H2ORDD[A] ) ( context: TaskContext, it: Iterator[A] ): Int = {
    println(context.partitionId+", name: "+h2ordd.h2oName)
    val jc = implicitly[ClassTag[A]].runtimeClass
    val flds = h2ordd.colNames.map(name => { val fld = jc.getDeclaredField(name); fld.setAccessible(true); fld }).toArray
    val prims= flds.map(_.getType.isPrimitive)
    val opts = flds.map(_.getType.isAssignableFrom(classOf[Option[_]]))
    val nums = flds.map(_.getType.isAssignableFrom(classOf[Number]))
    it.foreach(row => {
      val ds = for( i <- 0 until flds.length ) yield {
        if( prims(i) ) flds(i).getDouble(row)
        else if( opts(i) ) flds(i).get(row).asInstanceOf[Option[_]].getOrElse(Double.NaN)
        else if( nums(i) ) flds(i).get(row).asInstanceOf[Number].doubleValue
        else Double.NaN
      }
      ds
    })

    context.partitionId
  }

  // Returns the original RDD (evaluated), and as a side-effect makes an H2O Frame with the RDD data
  // Eager, not lazy, evaluation
  def createH2ORDD[A: TypeTag: ClassTag](rdd: RDD[A], name: String): H2ORDD[A] = {
    // Pull out the RDD members; the names become H2O column names; the type guide conversion
    val tt = typeOf[A].members.sorted.filter(!_.isMethod).toArray
    val names = tt.map(_.name.toString.trim)

    // Make an H2ORDD, and within it an H2O data Frame - but with no backing data (yet)
    val h2ordd = new H2ORDD(name, names, this, this.sparkContext, rdd)
    h2ordd.fr.preparePartialFrame(names)

    // Parallel-fill the H2O data Chunks
    sparkContext.runJob(h2ordd, perPartition[A](h2ordd) _) // eager, not lazy, evaluation

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    //h2ordd.fr.finizalizePartialFrame()
    h2ordd
  }
}
