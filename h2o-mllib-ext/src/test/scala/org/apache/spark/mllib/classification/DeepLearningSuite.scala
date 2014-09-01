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

package org.apache.spark.mllib.classification

import java.lang.Exception

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.schemas.DeepLearningV2.DeepLearningParametersV2
import org.apache.spark.h2o.H2OContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.util.{LocalH2OContext, LocalClusterSparkContext, LocalSparkContext}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConversions._
import scala.util.Random

object DeepLearningSuite {

  def generateLogisticInputAsList(
    offset: Double,
    scale: Double,
    nPoints: Int,
    seed: Int): java.util.List[ThePoint] = {
    seqAsJavaList(generateLogisticInput(offset, scale, nPoints, seed))
  }

  // Generate input of the form Y = logistic(offset + scale*X)
  def generateLogisticInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[ThePoint]  = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val y = (0 until nPoints).map { i =>
      val p = 1.0 / (1.0 + math.exp(-(offset + scale * x1(i))))
      if (rnd.nextDouble() < p) 1.0 else 0.0
    }

    val testData = (0 until nPoints).map(i => ThePoint(x1(i), y(i)) )
    testData
  }
}

// Helper class used instead of Labeled point
case class ThePoint(x:Double, y:Double)

class DeepLearningSuite extends FunSuite with LocalSparkContext with LocalH2OContext with Matchers {

  // Test if we can correctly learn A, B where Y = logistic(A + B*X)
  test("deep learning log regression") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    // Generate testing data
    val trainData = DeepLearningSuite.generateLogisticInput(A, B, nPoints, 42)
    // Create RDD from testing data
    val trainRDD = sc.parallelize(trainData, 2)
    trainRDD.cache()

    val hc = new H2OContext(sc)
    import hc._
    val trainH2ORDD = createH2ORDD(trainRDD, "train_data_rdd.hex")
    // Launch Deep Learning:
    // - configure parameters
    val dlParams = new DeepLearningParameters()
    val testRawH2OFrame = trainH2ORDD.fr

    dlParams.source = testRawH2OFrame
    dlParams.response = testRawH2OFrame.lastVec()
    dlParams.classification = true

    // - create a model builder
    val dl = new DeepLearning(dlParams)
    val dlModel = dl
                    .train()
                    .get()

    val validationData = DeepLearningSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    val validationH2ORDD = createH2ORDD(validationRDD, "validation_data_rdd.hex")
    val validationRawH2OFrame = validationH2ORDD.fr
    // Score validation data
    val predictionH2OFrame = dlModel.score(validationRawH2OFrame)

    //validationRDD.zip(prediction).map()

//    val validationH2ORDD = createH2ORDD(validatePrediction())
//    dlModel.score()
    // This is way of MLLIB
    /*val model = lr.run(testRDD)

    // Test the weights
    assert(model.weights(0) ~== -1.52 relTol 0.01)
    assert(model.intercept ~== 2.00 relTol 0.01)

    val validationData = DeepLearningSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData) */
  }

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected.label
    }
    // At least 83% of the predictions should be on.
    ((input.length - numOffPredictions).toDouble / input.length) should be > 0.83
  }
}
