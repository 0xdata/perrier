package org.apache.spark.examples.h2o

import java.util.Properties
import hex.schemas.KMeansV2
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import water.{Model, AutoBuffer}

object H2OTest {

  def main(args: Array[String]) {

    // Create Spark context which will drive computation
    // By default we use local spark context (which is useful for development)
    // but for cluster spark context, you should pass VM options -Dspark.master=spark://localhost:7077
    val sc = createSparkContext()

    // Start H2O-in-Spark
    if (sc.conf.get("spark.master").startsWith("local")) {
      water.H2OApp.main2("../h2o-dev")
      water.H2O.waitForCloudSize(1 /*One H2ONode to match the one Spark local-mode worker*/ , 1000)
    }

    // Load raw data
    val parse = ProstateParse
    val rawdata = sc.textFile("h2o-examples/smalldata/prostate.csv",2)
    // Parse raw data per line and produce
    val sqlContext = new SQLContext(sc)
    import sqlContext._ // import implicit conversions
    val table = rawdata.map(_.split(",")).map(line => parse(line))
    table.registerTempTable("prostate_table")

    // Invoke query on a sample of data
    val query = "SELECT * FROM prostate_table WHERE capsule=1"
    val result = sql(query) // Using a registered context and tables

    // Map data into H2O frame and run an algorithm
    val hc = new H2OContext(sc)

    // Register RDD as a frame which will cause data transfer
    //  - Invoked on result of SQL query, hence SQLSchema is used
    //  - This needs RDD -> H2ORDD implicit conversion, H2ORDDLike contains registerFrame
    // This will not work so far:
    // val h2oFrame = hc.createH2ORDD(result, "prostate.hex")
    val h2oFrame = hc.createH2ORDD(table, "prostate.hex")

    // Build a KMeansV2 model, setting model parameters via a Properties
    val props = new Properties
    for ((k,v) <- Seq("K"->"3")) props.setProperty(k,v)
    val job = new KMeansV2().fillFromParms(props).createImpl(h2oFrame.fr)
    val kmm = job.train().get()
    job.remove()
    // Print the JSON model
    println(new String(kmm._output.writeJSON(new AutoBuffer()).buf()))

    // Stop Spark local worker; stop H2O worker
    sc.stop()
    water.H2O.exit(0)
  }

  private def createSparkContext(sparkMaster:String = null): SparkContext = {
    // Create application configuration
    val conf = new SparkConf()
      .setAppName("H2O Integration Example")
      //.set("spark.executor.memory", "1g")
    //if (!local) // Run 'sbt assembly to produce target/scala-2.10/h2o-sparkling-demo-assembly-1.0.jar
    //  conf.setJars(Seq("h2o-examples/target/spark-h2o-examples_2.10-1.1.0-SNAPSHOT.jar"))
    if (System.getProperty("spark.master")==null) conf.setMaster("local")
    new SparkContext(conf)
  }
}

/** Prostate schema definition. */
case class Prostate(id      :Option[Int],
                    capsule :Option[Int],
                    age     :Option[Int],
                    race    :Option[Int],
                    dpros   :Option[Int],
                    dcaps   :Option[Int],
                    psa     :Option[Float],
                    vol     :Option[Float],
                    gleason :Option[Int])

/** A dummy csv parser for prostate dataset. */
object ProstateParse extends Serializable {
  def apply(row: Array[String]): Prostate = {
    import SchemaUtils._
    Prostate(int(row(0)), int(row(1)), int(row(2)), int(row(3)), int(row(4)), int(row(5)), float(row(6)), float(row(7)), int(row(8)) )
  }
}

/** Simple support for parsing data. */
object SchemaUtils {

  def int  (s:String):Option[Int]     = if (isValid(s)) parseInt(s)   else None
  def float(s:String):Option[Float]   = if (isValid(s)) parseFloat(s) else None
  def str  (s:String):Option[String]  = if (isValid(s)) Option(s)     else None
  def bool (s:String):Option[Boolean] = if (isValid(s)) parseBool(s)  else None

  def parseInt(s:String):Option[Int] =
    try { Option(s.trim().toInt) } catch {
      case e:NumberFormatException => None
    }

  def parseFloat(s:String):Option[Float] =
    try { Option(s.trim().toFloat) } catch {
      case e:NumberFormatException => None
    }

  def parseBool(s:String):Option[Boolean] = s.trim().toLowerCase match {
    case "true"|"yes" => Option(true)
    case "false"|"no" => Option(false)
    case _ => None
  }

  def isNA(s:String) = s==null || s.isEmpty || (s.trim.toLowerCase match {
    case "na" => true
    case "n/a"=> true
    case _ => false
  })

  def isValid(s:String) = !isNA(s)

  //def na[S]:S = null.asInstanceOf[S]
  //def na[S]:Option[S] = None
}

