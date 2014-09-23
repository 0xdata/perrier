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

import org.apache.spark.rdd.{H2ORDD, RDD}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{Row, SchemaRDD}
import org.apache.spark.{SparkContext, TaskContext}
import water.fvec.{DataFrame, Frame}
import water.{DKV, Key}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Simple H2O context motivated by SQLContext.
 *
 * Doing - implicit conversion from RDD -> H2OLikeRDD
 */
class H2OContext(@transient val sparkContext: SparkContext)
  extends org.apache.spark.Logging
  with H2OConf
  with Serializable {

  /** Implicit conversion from SchemaRDD to H2O's DataFrame */
  implicit def createDataFrame(rdd : SchemaRDD) : DataFrame = toDataFrame(rdd)

  /** Implicit conversion from typed RDD to H2O's DataFrame */
  implicit def createDataFrame[A <: Product : TypeTag](rdd : RDD[A]) : DataFrame = toDataFrame(rdd)

  /** Implicit conversion from SchemaRDD to H2O's DataFrame */
  implicit def createDataFrameKey(rdd : SchemaRDD) : Key = toDataFrame(rdd)._key

  /** Implicit conversion from typed RDD to H2O's DataFrame */
  implicit def createDataFrameKey[A <: Product : TypeTag](rdd : RDD[A]) : Key = toDataFrame(rdd)._key

  /** Implicit conversion from Frame to DataFrame */
  implicit def createDataFrame(fr: Frame) : DataFrame = new DataFrame(fr)

  implicit def dataFrameToKey(fr: Frame): Key = fr._key

  def toDataFrame(rdd: SchemaRDD) : DataFrame = H2OContext.toDataFrame(sparkContext, rdd)

  def toDataFrame[A <: Product : TypeTag](rdd: RDD[A]) : DataFrame = H2OContext.toDataFrame(sparkContext, rdd)

  def toRDD[A <: Product: TypeTag: ClassTag]( fr : DataFrame ) : RDD[A] = new H2ORDD[A](this,fr)

  /** Execute given code as Spark job */
  def |>[T]( code: => Unit)(implicit rdd:RDD[T]): Unit = {
    code
  }
}

object H2OContext {

  def toDataFrame(sc: SparkContext, rdd: SchemaRDD) : DataFrame = {
    val names = rdd.schema.fieldNames.toArray
    val types = rdd.schema.fields.map( field => dataTypeToClass(field.dataType) ).toArray

    // Make an H2O data Frame - but with no backing data (yet) - it has to be run in target environment
    val keyName = Key.rand // FIXME put there a name based on RDD
    // FIXME: expects number of partitions > 0
    // FIXME: here we are simulating RPC call, better would be contact H2O node directly via backend
    // - in this case we have to wait since the backend is ready and then ask for location of executor
    println("Before preparing frame referenced by ket " + keyName)
    sc.runJob(rdd,
      initFrame(keyName, names) _ ,
      Seq(0), // Invoke code only on node with 1st partition
      false) // Do not allow for running in driver locally

    val rows = sc.runJob(rdd, perSQLPartition(keyName, types) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.size)
    rows.foreach{ case(cidx,nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    sc.runJob(rdd,
      finalizeFrame(keyName, res) _,
      Seq(0),
      false
    )
    null
  }

  private def initFrame[T](keyName: String, names: Array[String])(it:Iterator[T]):Unit = {
    val fr = new water.fvec.Frame(keyName)
    fr.preparePartialFrame(names)
    // Save it directly to DKV
    fr.update(null)
  }
  private def finalizeFrame[T](keyName: String, res: Array[Long])(it:Iterator[T]):Unit = {
    val fr:Frame = DKV.get(keyName).get.asInstanceOf[Frame]
    fr.finalizePartialFrame(res)
  }

  def toDataFrame[A <: Product : TypeTag](sc: SparkContext, rdd: RDD[A]) : DataFrame = {
    import org.apache.spark.h2o.ReflectionUtils._
    val fnames = names[A]

    // Make an H2O data Frame - but with no backing data (yet)
    val keyName = Key.rand
    sc.runJob(rdd,
      initFrame(keyName, names) _ ,
      Seq(0), // Invoke code only on node with 1st partition
      false) // Do not allow for running in driver locally

    val rows = sc.runJob(rdd, perRDDPartition(keyName) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.size)
    rows.foreach{ case(cidx,nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    // Add Vec headers per-Chunk, and finalize the H2O Frame
    sc.runJob(rdd,
      finalizeFrame(keyName, res) _,
      Seq(0),
      false
    )
    null
  }

  private def dataTypeToClass(dt : DataType):Class[_] = dt match {
    case BinaryType  => classOf[Integer]
    case IntegerType => classOf[Integer]
    case FloatType   => classOf[java.lang.Float]
    case DoubleType  => classOf[java.lang.Double]
    case _ => throw new IllegalArgumentException(s"Unsupported type $dt")
  }

  private
  def perSQLPartition ( keystr: String, types: Array[Class[_]] )
                      ( context: TaskContext, it: Iterator[Row] ): (Int,Long) = {
    val nchks = water.fvec.Frame.createNewChunks(keystr,context.partitionId)
    //println (nchks.length + " Types: " + types.mkString(","))
    it.foreach(row => {
      for( i <- 0 until types.length) {
        nchks(i).addNum(
          if (row.isNullAt(i)) Double.NaN
          else types(i) match {
            case q if q==classOf[Integer] => row.getInt(i)
            case q if q==classOf[java.lang.Double]  => row.getDouble(i)
            case q if q==classOf[java.lang.Float]   => row.getFloat(i)
            case _ => Double.NaN
          }
        )
      }
    })
    // Compress & write out the Partition/Chunks
    water.fvec.Frame.closeNewChunks(nchks)
    // Return Partition# and rows in this Partition
    (context.partitionId,nchks(0).len)
  }

  private
  def perRDDPartition[A<:Product]( keystr:String )
                                         ( context: TaskContext, it: Iterator[A] ): (Int,Long) = {
    // An array of H2O NewChunks; A place to record all the data in this partition
    val nchks = water.fvec.Frame.createNewChunks(keystr,context.partitionId)
    it.foreach(prod => { // For all rows which are subtype of Product
      for( i <- 0 until prod.productArity ) { // For all fields...
      val fld = prod.productElement(i)
        nchks(i).addNum( { // Copy numeric data from fields to NewChunks
        val x = fld match { case Some(n) => n; case _ => fld }
          x match {
            case n: Number => n.doubleValue
            case n: Boolean => if (n) 1 else 0
            case _ => Double.NaN
          }
        } )
      }
    })
    // Compress & write out the Partition/Chunks
    water.fvec.Frame.closeNewChunks(nchks)
    // Return Partition# and rows in this Partition
    (context.partitionId,nchks(0).len)
  }

  def toRDD[A <: Product: TypeTag: ClassTag]
           ( h2oContext : H2OContext, fr : DataFrame ) : RDD[A] = new H2ORDD[A](h2oContext,fr)
}
