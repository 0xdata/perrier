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

package org.apache.spark.h2o

import org.apache.spark.SparkConf

/**
 * Just simple configuration holder which is representing
 * properties passed from user to H2O App.
 */
trait H2OConf {

  /* Require Spar config */
  def sparkConf:SparkConf
  // Precondition
  require(sparkConf != null, "sparkConf was null")

  /* Initialize configuration */
  // Collect configuration properties
  def numH2OWorkers = sparkConf.getInt(PROP_CLUSTER_SIZE._1, PROP_CLUSTER_SIZE._2)
  def useFlatFile   = sparkConf.getBoolean(PROP_USE_FLATFILE._1, PROP_USE_FLATFILE._2)
  def basePort      = sparkConf.getInt(PROP_PORT_BASE._1, PROP_PORT_BASE._2)
  def incrPort      = sparkConf.getInt(PROP_PORT_INCR._1, PROP_PORT_INCR._2)
  def cloudTimeout  = sparkConf.getInt(PROP_CLOUD_TIMEOUT._1, PROP_CLOUD_TIMEOUT._2)
  def drddMulFactor = sparkConf.getInt(PROP_DUMMY_RDD_MUL_FACTOR._1, PROP_DUMMY_RDD_MUL_FACTOR._2)

  /* Configuration properties */

  /** Configuration property - use flatfile for H2O cloud formation. */
  val PROP_USE_FLATFILE = ( "spark.ext.h2o.flatfile", false)
  /** Configuration property - expected number of workers of H2O cloud. */
  val PROP_CLUSTER_SIZE = ( "spark.ext.h2o.cluster.size", 1 )
  /** Configuration property - base port used for individual H2O nodes configuration. */
  val PROP_PORT_BASE = ( "spark.ext.h2o.port.base", 54321 )
  /** Configuration property - increment added to base port to find available port. */
  val PROP_PORT_INCR = ( "spark.ext.h2o.port.incr", 2)
  /** Configuration property - timeout for cloud up. */
  val PROP_CLOUD_TIMEOUT = ("spark.ext.h2o.cloud.timeout", 60*1000)


  /** Configuration property - multiplication factor for dummy RDD generation.
    * Size of dummy RDD is PROP_CLUSTER_SIZE*PROP_DUMMY_RDD_MUL_FACTOR */
  val PROP_DUMMY_RDD_MUL_FACTOR = ("spark.ext.h2o.dummy.rdd.mul.factor", 10)

  override def toString: String =
    s"""Sparkling H2O setup:
         |  workers=$numH2OWorkers
         |  flatfile: $useFlatFile
         |  basePort: $basePort
         |  incrPort: $incrPort
         |  drddMulFactor: $drddMulFactor""".stripMargin

}
