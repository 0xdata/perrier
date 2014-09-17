package org.apache.spark.rdd

import org.apache.spark.h2o.H2OContext

/**
 * Contains functions that are shared between all H2ORDD types (i.e., Scala, Java)
 */
private[rdd] trait H2ORDDLike {
  @transient val h2oContext : H2OContext

  //private[rdd] def baseRDD: H2ORDD[]

}
