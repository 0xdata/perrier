package org.apache.spark.rdd

import org.apache.spark.h2o.H2OContext
import org.apache.spark.{TaskContext, Partition, SparkContext}
import water.fvec.Frame
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
private[spark]
class H2ORDD[A :ClassTag](val h2oName: String, val colNames: Array[String],
                          @transient val hc: H2OContext, @transient sc: SparkContext, rdd: RDD[A])
  extends RDD[A](sc, Nil) {

  /** H2O data Frame.  Lazily set from null to not-null on first use */
  @transient val fr: water.fvec.Frame = new water.fvec.Frame(h2oName)

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    if( rdd != null ) rdd.compute(split,context)
    else {
      new Iterator[A] {
        val fr : Frame = DKV.get(Key.make(h2oName)).get.asInstanceOf[Frame]
        val a : A = implicitly[ClassTag[A]].runtimeClass.newInstance.asInstanceOf[A]
        val chks = fr.getChunks(split.index)
        val nrows = chks(0).len
        var row : Int = 0
        def hasNext: Boolean = row < nrows
        def next(): A = {
          //for( i <- 0 until chks.length )
          //  a.
          a
        }
      }
    }
  }

  /** Pass thru an RDD if given one, else pull from the H2O Frame */
  override protected def getPartitions: Array[Partition] = {
    if( rdd != null ) rdd.partitions
    else { // Make partitions to draw from H2O Frame
      val num = fr.anyVec().nChunks()
      val res = new Array[Partition](num)
      for( i <- 0 until num ) res(i) = new Partition { val index = i }
      res
    }
  }

}
