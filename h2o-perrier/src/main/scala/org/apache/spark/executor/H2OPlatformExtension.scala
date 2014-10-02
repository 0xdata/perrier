/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import org.apache.spark.{ExtensionState, Logging, SparkConf, PlatformExtension}

/**
 * Spark extension launching H2O.
 *
 * FIXME: remove lifecycle state machine to a driver object
 */
class H2OPlatformExtension extends PlatformExtension with Logging {
  import ExtensionState._

  private var extstate = STOPPED

  /** Method to start extension */
  override def start(conf: SparkConf): Unit = {
    extstate = STARTING
    val h2oClusterTimeout = conf.getInt("spark.ext.h2o.cloud.timeout",
                              conf.getInt("spark.executorEnv.spark.ext.h2o.cloud.timeout", 60000 /*msec*/))

    logDebug("Starting H2O Spark Extension...")
    water.H2O.main(new Array[String](0))
    water.H2O.finalizeRequest()
    // FIXME we can continue only if all H2O nodes are ready
    // FIXME this is hack! We should figure out cloud size
    water.H2O.waitForCloudSize(conf.getInt("spark.ext.h2o.cluster.size", 1), h2oClusterTimeout)
    logDebug("H2O extension started.")
    extstate = STARTED
  }

  /** Method to stop extension */
  override def stop(conf: SparkConf): Unit = {
    extstate = STOPPING
    logDebug("Stopping H2O Spark Extension...")
    // do nothing since we have no shutdown
    logDebug("H2O extension stopped.")
    extstate = STOPPED
  }

  override def state = extstate

  /* description of extension */
  override def desc: String = "H2O Cluster Extension"
}
