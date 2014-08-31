package org.apache.spark.rdd

import org.apache.spark.h2o.H2OContext
import org.apache.spark.{TaskContext, Partition, SparkContext}
import water.fvec.DataFrame
import water.{DKV, Key}
import scala.reflect.ClassTag

/**
 * Testing support for H2O lazy RDD loading data from H2O KV-store.
 *
 * It should contain only RDD-related implementation, all support methods
 * should be offloaded into H2ORDDLike trait.
 *
 * NOTES:
 *  - spark context to contain h2o info
 *  - sparkcontext.initH2O() returns H2OContext
 *  - import H2OContext
 *  - registerRDDasFrame(RDD, "frame reference")
 *
 *  TODO: create JIRA support arbitrary spark runtime extension, arbitrary extension data
 *
 */

...
private[spark]
class H2ORDD[A: ClassTag](val h2oName: String,
             val colNames: Array[String],
             @transient val hc: H2OContext,
             @transient sc: SparkContext,
             @transient rdd: Option[RDD[A]],
             @transient fr:  Option[water.fvec.DataFrame])
  extends RDD[A](sc, Nil) {
    if( rdd.isEmpty && fr.isEmpty ) throw new IllegalArgumentException("Must pass in either an RDD or a DataFrame")

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    if( rdd != null ) rdd.get.compute(split,context)
    else {
      new Iterator[A] {
        val fr : DataFrame = DKV.get(Key.make(h2oName)).get.asInstanceOf[DataFrame]
        val a : A = implicitly[ClassTag[A]].runtimeClass.newInstance.asInstanceOf[A]
        val chks = fr.getChunks(split.index)
        val nrows = chks(0).len
        var row : Int = 0
        def hasNext: Boolean = row < nrows
        def next(): A = {
          ???
          //for( i <- 0 until chks.length )
          //  a.
          //a
        }
      }
    }
  }

  /** Pass thru an RDD if given one, else pull from the H2O Frame */
  override protected def getPartitions: Array[Partition] = {
    if( rdd != null ) rdd.get.partitions
    else { // Make partitions to draw from H2O Frame
      val num = fr.get.anyVec().nChunks()
      val res = new Array[Partition](num)
      for( i <- 0 until num ) res(i) = new Partition { val index = i }
      res
    }
  }

}
