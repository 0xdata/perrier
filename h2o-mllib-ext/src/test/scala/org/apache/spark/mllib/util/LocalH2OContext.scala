package org.apache.spark.mllib.util

import org.scalatest.{Suite, BeforeAndAfterAll}


trait LocalH2OContext extends BeforeAndAfterAll { self: Suite =>
  override def beforeAll() {
    // Start h2o - single node
    water.H2O.main(new Array[String](0))
    water.H2O.finalizeRequest()
    water.H2O.waitForCloudSize(1 /*One H2ONode to match the one Spark local-mode worker*/ , 1000)
    //
    super.beforeAll()
  }

  override def afterAll() {
    //water.H2O.shutdown
    super.afterAll()
  }
}
