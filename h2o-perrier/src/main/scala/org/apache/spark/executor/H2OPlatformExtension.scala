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

import org.apache.spark.{SparkConf, PlatformExtension}

/**
 * Spark extension launching H2O.
 */
class H2OPlatformExtension extends PlatformExtension {
  /** Method to start extension */
  override def start(conf: SparkConf): Unit = {
    water.H2O.main(new Array[String](0))
    water.H2O.finalizeRequest()
  }

  /** Method to stop extension */
  override def stop: Unit = {
    // do nothing since we have no shutdown
  }

  /* description of extension */
  override def desc: String = "H2O Cluster Extension"
}
