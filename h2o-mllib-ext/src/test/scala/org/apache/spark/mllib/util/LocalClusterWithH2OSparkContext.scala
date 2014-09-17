package org.apache.spark.mllib.util

import org.apache.spark.executor.H2OPlatformExtension
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}


trait LocalClusterWithH2OSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local-cluster[2, 1, 512]")
      .setAppName("test-cluster")
      .set("spark.akka.frameSize", "1") // set to 1MB to detect direct serialization of data
      .set("spark.h2o.cluster.size", "2")
      .addExtension[H2OPlatformExtension] // Add H2O extension
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }
}
