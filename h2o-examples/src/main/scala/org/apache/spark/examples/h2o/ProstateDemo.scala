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

package org.apache.spark.examples.h2o

import hex.kmeans.KMeans
import hex.kmeans.KMeansModel.KMeansParameters
import org.apache.spark.executor.H2OPlatformExtension
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import water._
import water.fvec.DataFrame

/* Demonstrates:
   - data transfer from RDD into H2O
   - algorithm call
 */
object ProstateDemo {

  def main(args: Array[String]) {

    // Create Spark context which will drive computation
    // By default we use local spark context (which is useful for development)
    // but for cluster spark context, you should pass
    // VM option -Dspark.master=spark://localhost:7077 or via shell variable MASTER
    val sc = createSparkContext()
    // Add a file to be available for cluster mode
    // FIXME: absolute path is here because in cluster deployment mode the file is not found (JVM path is different from .)
    sc.addFile("/Users/michal/Devel/projects/h2o/repos/perrier/h2o-examples/smalldata/prostate.csv")

    // We do not need to wait for H2O cloud since it will be launched by backend

    // Load raw data
    val parse = ProstateParse
    val rawdata = sc.textFile(SparkFiles.get("prostate.csv"), 2)
    // Parse data into plain RDD[Prostate]
    val table = rawdata.map(_.split(",")).map(line => parse(line))

    // Convert to SQL type RDD
    val sqlContext = new SQLContext(sc)
    import sqlContext._ // import implicit conversions
    table.registerTempTable("prostate_table")

    // Invoke query on data; select a subsample
    val query = "SELECT * FROM prostate_table WHERE CAPSULE=1"
    val result = sql(query) // Using a registered context and tables

    // We would like to use H2O RDDs
    val h2oContext = new H2OContext(sc)
    import h2oContext._

    // Build a KMeans model, setting model parameters via a Properties
    val model:Array[KMeansModelPOJO] = sc.runJob(result,
      runKmeans(result) _, // Use implicit conversion
      Seq(0),
      false
    )
    println(model.mkString(","))

    // FIXME: shutdown H2O cloud not JVMs since we are embedded inside Spark JVM
    // Stop Spark local worker; stop H2O worker
    sc.stop()
  }

  /* BIG HACK: fetching type map to achieve H2O Iced deserialization
  private def fetchTypeMap[T](it: Iterator[T]): java.util.Map[String, Integer] = {
    // dummy load
    new KMeansModel()
    val map = TypeMap.MAP
    import scala.collection.JavaConversions._
    map.foreach { e => println (s"${e._1} = ${e._2}") }
    map
  }

  private def xx(rdd:RDD[_], sc: SparkContext):Unit = {
    val maps = sc.runJob(rdd, fetchTypeMap _, Seq(0), false)
    val map = maps(0)
    TypeMap.reinstall(map)
  }*/

  private def runKmeans[T](trainDataFrame: DataFrame)(it : Iterator[T]): KMeansModelPOJO = {
    val params = new KMeansParameters
    params._training_frame = trainDataFrame._key
    params._K = 3
    // Create a builder
    val job = new KMeans(params)
    // Launch a job and wait for the end.
    val kmm = job.train().get()
    job.remove()
    // Print the JSON model
    println(new String(kmm._output.writeJSON(new AutoBuffer()).buf()))
    // FIXME: we cannot return directly KMeansModel since it is serialized by internal Icer (even it is Java based serialization)
    // Possibilities: return a holder for model | return JSON | have proper client mode
    new KMeansModelPOJO
  }

  private def createSparkContext(sparkMaster:String = null): SparkContext = {
    // Create application configuration
    val conf = new SparkConf()
      .setAppName("H2O Integration Example")
      //.set("spark.executor.memory", "1g")
    if (System.getProperty("spark.master")==null) conf.setMaster("local")
    conf.set("spark.h2o", "true")
    // For local development always wait for cloud of size 1
    conf.set("spark.h2o.cluster.size", if (conf.get("spark.master").startsWith("local")) "1" else "2")
    conf.set("spark.eventLog.enabled ", "true")
    conf.set("spark.eventLog.dir", "/Tmp/spark-app")

    conf.addExtension[H2OPlatformExtension]

    new SparkContext(conf)
  }

  class KMeansModelPOJO extends Serializable {
  }
}

/** Prostate schema definition. */
case class Prostate(ID      :Option[Int]  ,
                    CAPSULE :Option[Int]  ,
                    AGE     :Option[Int]  ,
                    RACE    :Option[Int]  ,
                    DPROS   :Option[Int]  ,
                    DCAPS   :Option[Int]  ,
                    PSA     :Option[Float],
                    VOL     :Option[Float],
                    GLEASON :Option[Int]  ) {
}

/** A dummy csv parser for prostate dataset. */
object ProstateParse extends Serializable {
  def apply(row: Array[String]): Prostate = {
    import org.apache.spark.examples.h2o.SchemaUtils._
    Prostate(int(row(0)), int(row(1)), int(row(2)), int(row(3)), int(row(4)), int(row(5)), float(row(6)), float(row(7)), int(row(8)) )
  }
}


