package org.apache.spark.examples.h2o

import java.io.File

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import org.apache.spark.executor.H2OPlatformExtension
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import water.{H2O, H2OApp}
import water.fvec.{MapReduce, DataFrame}


object DeepLearningDemo {

  def main(args: Array[String]): Unit = {
    // Create Spark context which will drive computation.
    val sc = createSparkContext()

    //
    // Load H2O from CSV file (i.e., access directly H2O cloud)
    // Use super-fast advanced H2O CSV parser !!!
    val dataFile = "/Users/michal/Devel/projects/h2o/repos/NEW.h2o.github/smalldata/airlines/allyears2k_headers.zip"
    println(s"===> Parsing datafile: $dataFile")
    val airlinesData = new DataFrame(new File(dataFile))

    //
    // Use H2O to RDD transformation
    //
    val h2oContext = new H2OContext(sc)
    import h2oContext._
    val airlinesTable : RDD[Airlines] = toRDD[Airlines](airlinesData)
    println(s"\n===> Number of all flights via RDD#count call: ${airlinesTable.count()}\n")
    println(s"\n===> Number of all flights via H2O#Frame#count: ${airlinesData.numRows()}\n")

    //
    // Filter data with help of Spark SQL
    //

    val sqlContext = new SQLContext(sc)
    import sqlContext._ // import implicit conversions
    airlinesTable.registerTempTable("airlinesTable")

    // Select only interesting columns and flights with destination in SFO
    val query = "SELECT * FROM airlinesTable WHERE Dest LIKE 'SFO'"
    val result = sql(query) // Using a registered context and tables
    println(s"\n===> Number of flights with destination in SFO: ${result.count()}\n")

    //
    // Run Deep Learning
    //

    // Configure Deep Learning algorithm
    val dlParams = new DeepLearningParameters()
    dlParams._training_frame = airlinesData( 'Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
                                    'UniqueCarrier, 'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest,
                                    'Distance, 'IsDepDelayed)
    dlParams.response_column = 'IsDepDelayed.name
    dlParams.classification = true

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.train.get

    //
    // Use model for scoring
    //
    val predictionH2OFrame = dlModel.score(airlinesData)('predict)
    val predictionsFromModel = toRDD[Result](predictionH2OFrame).take(10).map ( _.predict.getOrElse("NaN") )
    println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

    // Stop Spark cluster and destroy all executors
    sc.stop()
    // This will block in cluster mode since we have H2O launched in driver
  }

  private def createSparkContext(sparkMaster:String = null): SparkContext = {
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

  case class Result(predict: Option[Double])
}
