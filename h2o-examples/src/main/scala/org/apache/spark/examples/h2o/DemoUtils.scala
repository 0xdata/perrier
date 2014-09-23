package org.apache.spark.examples.h2o

import org.apache.spark.executor.H2OPlatformExtension
import org.apache.spark.{SparkConf, SparkContext}
import water.{H2O, H2OApp}

/**
 * Shared demo utility functions.
 */
private[h2o] object DemoUtils {

  def createSparkContext(sparkMaster:String = null): SparkContext = {
    val h2oWorkers = System.getProperty("spark.h2o.workers", "3") // N+1 workers, one is running in driver
    //
    // Create application configuration
    //
    val conf = new SparkConf()
        .setAppName("H2O Integration Example")
    if (System.getProperty("spark.master")==null) conf.setMaster("local")
    // Setup executor memory directly here
    conf.set("spark.executor.memory", "3g")
    // For local development always wait for cloud of size 1
    conf.set("spark.h2o.cluster.size", if (conf.get("spark.master").startsWith("local")) "1" else h2oWorkers)
    //
    // Setup H2O extension of Spark platform proposed by SPARK JIRA-3270
    //
    conf.addExtension[H2OPlatformExtension] // add H2O extension

    val sc = new SparkContext(conf)
    //
    // In non-local case we create a small h2o instance in driver to have access to the c,oud
    //
    if (!sc.isLocal) {
      println("Waiting for " + h2oWorkers)
      H2OApp.main(Array("-client"))
      H2O.waitForCloudSize( h2oWorkers.toInt /* One H2ONode to match the one Spark worker and one is running in driver*/
        , 10000)
    } else {
      // Since LocalBackend does not wait for initialization (yet)
      H2O.waitForCloudSize(1, 1000)
    }
    sc
  }


}
