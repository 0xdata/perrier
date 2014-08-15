package org.apache.spark.rdd

import org.apache.spark.h2o.H2OContext
import org.apache.spark.{TaskContext, Partition, SparkContext}
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
class H2ORDD[A :ClassTag](val h2oName: String, val colNames: Array[String], val colTypes: Array[String],
                          @transient val hc: H2OContext, @transient sc: SparkContext, rdd: RDD[A])
  extends RDD[A](sc, Nil) {

  /** H2O data Frame.  Lazily set from null to not-null on first use */
  @transient val fr: water.fvec.Frame = new water.fvec.Frame()

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = rdd.compute(split,context)

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = rdd.partitions
}
