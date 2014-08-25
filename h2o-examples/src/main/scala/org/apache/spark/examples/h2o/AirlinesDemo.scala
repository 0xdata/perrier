package org.apache.spark.examples.h2o

import java.io.File
import java.util.Properties
import hex.schemas.KMeansV2
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rdd.{H2ORDD, RDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import water.fvec.DataFrame

object AirlinesDemo {

  def main(args: Array[String]) {

    // Create Spark context which will drive computation
    // By default we use local spark context (which is useful for development)
    // but for cluster spark context, you should pass VM options -Dspark.master=spark://localhost:7077
    val sc = createSparkContext()
    val sqlContext = new SQLContext(sc)
    import sqlContext._ // import implicit conversions

    // Start H2O-in-Spark
    if (sc.conf.get("spark.master").startsWith("local")) {
      water.H2OApp.main2("../h2o-dev")
      water.H2O.waitForCloudSize(1 /*One H2ONode to match the one Spark local-mode worker*/ , 1000)
    }

    // Load data into H2O
    val hc = new H2OContext(sc)
    val airlines = new DataFrame(new File("h2o-examples/smalldata/allyears2k_headers.csv.gz"))
    //val h2ordd = hc.parse[Airlines]("airlines.hex",
    //println(h2ordd.take(1)(0))

    //// Load raw data
    //val parse = AirlinesParse
    //val rawdata = sc.textFile("h2o-examples/smalldata/allyears2k_headers.csv.gz",2)
    //// Drop the header line
    //val noheaderdata = rawdata.mapPartitionsWithIndex((partitionIdx: Int, lines: Iterator[String]) => {
    //  if (partitionIdx == 0) lines.drop(1)
    //  lines
    //})
    //
    //// Parse raw data per line and produce RDD of Airlines objects
    //val table : RDD[Airlines] = noheaderdata.map(_.split(",")).map(line => parse(line))
    //table.registerTempTable("airlines_table")
    //println(table.take(2).map(_.toString).mkString("\n"))
    //
    //// Invoke query on a sample of data
    ////val query = "SELECT * FROM airlines_table WHERE capsule=1"
    ////val result = sql(query) // Using a registered context and tables
    //
    //// Map data into H2O frame and run an algorithm
    //
    //// Register RDD as a frame which will cause data transfer
    ////  - Invoked on result of SQL query, hence SQLSchema is used
    ////  - This needs RDD -> H2ORDD implicit conversion, H2ORDDLike contains registerFrame
    //// This will not work so far:
    //val h2oFrame = hc.createH2ORDD(table, "airlines.hex")

    // Build a KMeansV2 model, setting model parameters via a Properties
    //val props = new Properties
    //for ((k,v) <- Seq("K"->"3")) props.setProperty(k,v)
    //val job = new KMeansV2().fillFromParms(props).createImpl(h2ordd.fr)
    //val kmm = job.train().get()
    //job.remove()
    //// Print the JSON model
    //println(new String(kmm._output.writeJSON(new AutoBuffer()).buf()))

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

/** Airlines schema definition. */
class Airlines (Year              :Option[Int],
                Month             :Option[Int],
                DayofMonth        :Option[Int],
                DayOfWeek         :Option[Int],
                DepTime           :Option[Int],
                CRSDepTime        :Option[Int],
                ArrTime           :Option[Int],
                CRSArrTime        :Option[Int],
                UniqueCarrier     :Option[String],
                FlightNum         :Option[Int],
                TailNum           :Option[Int],
                ActualElapsedTime :Option[Int],
                CRSElapsedTime    :Option[Int],
                AirTime           :Option[Int],
                ArrDelay          :Option[Int],
                DepDelay          :Option[Int],
                Origin            :Option[String],
                Dest              :Option[String],
                Distance          :Option[Int],
                TaxiIn            :Option[Int],
                TaxiOut           :Option[Int],
                Cancelled         :Option[Int],
                CancellationCode  :Option[Int],
                Diverted          :Option[Int],
                CarrierDelay      :Option[Int],
                WeatherDelay      :Option[Int],
                NASDelay          :Option[Int],
                SecurityDelay     :Option[Int],
                LateAircraftDelay :Option[Int],
                IsArrDelayed      :Option[Boolean],
                IsDepDelayed      :Option[Boolean]) extends Product {

  def this() = this(None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None)
  override def canEqual(that: Any):Boolean = that.isInstanceOf[Airlines]
  override def productArity: Int = 31
  override def productElement(n: Int) = n match {
    case  0 => Year
    case  1 => Month
    case  2 => DayofMonth
    case  3 => DayOfWeek
    case  4 => DepTime
    case  5 => CRSDepTime
    case  6 => ArrTime
    case  7 => CRSArrTime
    case  8 => UniqueCarrier
    case  9 => FlightNum
    case 10 => TailNum
    case 11 => ActualElapsedTime
    case 12 => CRSElapsedTime
    case 13 => AirTime
    case 14 => ArrDelay
    case 15 => DepDelay
    case 16 => Origin
    case 17 => Dest
    case 18 => Distance
    case 19 => TaxiIn
    case 20 => TaxiOut
    case 21 => Cancelled
    case 22 => CancellationCode
    case 23 => Diverted
    case 24 => CarrierDelay
    case 25 => WeatherDelay
    case 26 => NASDelay
    case 27 => SecurityDelay
    case 28 => LateAircraftDelay
    case 29 => IsArrDelayed
    case 30 => IsDepDelayed
    case  _ => throw new IndexOutOfBoundsException(n.toString)
  }
  override def toString:String = {
    val sb = new StringBuffer
    for( i <- 0 until productArity )
      sb.append(productElement(i)).append(',')
    sb.toString
  }
}

/** A dummy csv parser for airlines dataset. */
object AirlinesParse extends Serializable {
  def apply(row: Array[String]): Airlines = {
    import SchemaUtils._
    new Airlines(int (row( 0)), // Year
                 int (row( 1)), // Month
                 int (row( 2)), // DayofMonth
                 int (row( 3)), // DayOfWeek
                 int (row( 4)), // DepTime
                 int (row( 5)), // CRSDepTime
                 int (row( 6)), // ArrTime
                 int (row( 7)), // CRSArrTime
                 str (row( 8)), // UniqueCarrier
                 int (row( 9)), // FlightNum
                 int (row(10)), // TailNum
                 int (row(11)), // ActualElapsedTime
                 int (row(12)), // CRSElapsedTime
                 int (row(13)), // AirTime
                 int (row(14)), // ArrDelay
                 int (row(15)), // DepDelay
                 str (row(16)), // Origin
                 str (row(17)), // Dest
                 int (row(18)), // Distance
                 int (row(19)), // TaxiIn
                 int (row(20)), // TaxiOut
                 int (row(21)), // Cancelled
                 int (row(22)), // CancellationCode
                 int (row(23)), // Diverted
                 int (row(24)), // CarrierDelay
                 int (row(25)), // WeatherDelay
                 int (row(26)), // NASDelay
                 int (row(27)), // SecurityDelay
                 int (row(28)), // LateAircraftDelay
                 bool(row(29)), // IsArrDelayed
                 bool(row(30))) // IsDepDelayed
  }
}
