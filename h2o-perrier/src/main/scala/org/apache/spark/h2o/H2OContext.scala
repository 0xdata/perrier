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

import java.util

import org.apache.spark.rdd.{H2ORDD, RDD}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{Row, SchemaRDD}
import org.apache.spark.{Accumulable, SparkContext, TaskContext}
import water.fvec.{DataFrame, Frame}
import water.parser.ValueString
import water.{DKV, Key}

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Simple H2O context motivated by SQLContext.
 *
 * Doing - implicit conversion from RDD -> H2OLikeRDD
 *
 * FIXME: unify path for RDD[A] and SchemaRDD
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
  implicit def createDataFrameKey[A <: Product : TypeTag](rdd : RDD[A]) : Key
                                  = toDataFrame(rdd)._key

  /** Implicit conversion from Frame to DataFrame */
  implicit def createDataFrame(fr: Frame) : DataFrame = new DataFrame(fr)

  implicit def dataFrameToKey(fr: Frame): Key = fr._key

  def toDataFrame(rdd: SchemaRDD) : DataFrame = H2OContext.toDataFrame(sparkContext, rdd)

  def toDataFrame[A <: Product : TypeTag](rdd: RDD[A]) : DataFrame
                                  = H2OContext.toDataFrame(sparkContext, rdd)

  def toRDD[A <: Product: TypeTag: ClassTag]( fr : DataFrame ) : RDD[A] = new H2ORDD[A](this,fr)
}

object H2OContext {

  /** Transform SchemaRDD into H2O DataFrame */
  def toDataFrame(sc: SparkContext, rdd: SchemaRDD) : DataFrame = {
    val fnames = rdd.schema.fieldNames.toArray
    val ftypes = rdd.schema.fields.map( field => dataTypeToClass(field.dataType) ).toArray

    // Collect domains for String columns
    val fdomains = collectColumnDomains(sc, rdd, fnames, ftypes)

    val keyName = Key.rand // FIXME put there a name based on RDD
    // FIXME: expects number of partitions > 0
    initFrame(keyName, fnames)

    val rows = sc.runJob(rdd, perSQLPartition(keyName, ftypes, fdomains) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.size)
    rows.foreach { case(cidx,nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new DataFrame(finalizeFrame(keyName, res, ftypes, fdomains))
  }

  private def initFrame[T](keyName: String, names: Array[String]):Unit = {
    val fr = new water.fvec.Frame(keyName)
    fr.preparePartialFrame(names)
    // Save it directly to DKV
    fr.update(null)
  }
  private def finalizeFrame[T](keyName: String,
                               res: Array[Long],
                               colTypes: Array[Class[_]],
                               colDomains: Array[Array[String]]):Frame = {
    val fr:Frame = DKV.get(keyName).get.asInstanceOf[Frame]
    fr.finalizePartialFrame(res, colTypes, colDomains)
    fr
  }

  /** Transform typed RDD into H2O DataFrame */
  def toDataFrame[A <: Product : TypeTag](sc: SparkContext, rdd: RDD[A]) : DataFrame = {
    import org.apache.spark.h2o.ReflectionUtils._
    val fnames = names[A]
    val ftypes = types[A]
    // Collect domains for string columns
    val fdomains = collectColumnDomains(sc, rdd, fnames, ftypes)

    // Make an H2O data Frame - but with no backing data (yet)
    val keyName = Key.rand
    initFrame(keyName, fnames)

    val rows = sc.runJob(rdd, perRDDPartition(keyName) _) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.size)
    rows.foreach{ case(cidx,nrows) => res(cidx) = nrows }

    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new DataFrame(finalizeFrame(keyName, res, types, fdomains))
  }

  private def dataTypeToClass(dt : DataType):Class[_] = dt match {
    case BinaryType  => classOf[Integer]
    case IntegerType => classOf[Integer]
    case FloatType   => classOf[java.lang.Float]
    case DoubleType  => classOf[java.lang.Double]
    case StringType  => classOf[String]
    case BooleanType => classOf[java.lang.Boolean]
    case _ => throw new IllegalArgumentException(s"Unsupported type $dt")
  }

  private
  def perSQLPartition ( keystr: String, types: Array[Class[_]], domains: Array[Array[String]] )
                      ( context: TaskContext, it: Iterator[Row] ): (Int,Long) = {
    val nchks = water.fvec.Frame.createNewChunks(keystr,context.partitionId)
    val domHash = domains.map( ary =>
      if (ary==null) {
        null.asInstanceOf[mutable.Map[String,Int]]
      } else {
        val m = new mutable.HashMap[String, Int]()
        for (idx <- ary.indices) m.put(ary(idx), idx)
        m
      })
    it.foreach(row => {
      val valStr = new ValueString()
      for( i <- 0 until types.length) {
        val nchk = nchks(i)
        if (row.isNullAt(i)) nchk.addNA()
        else types(i) match {
          case q if q==classOf[Integer]           => nchk.addNum(row.getInt(i))
          case q if q==classOf[java.lang.Double]  => nchk.addNum(row.getDouble(i))
          case q if q==classOf[java.lang.Float]   => nchk.addNum(row.getFloat(i))
          case q if q==classOf[java.lang.Boolean] => nchk.addNum(if (row.getBoolean(i)) 1 else 0)
          case q if q==classOf[String]            =>
            if (domains(i)==null) nchk.addStr(valStr.setTo(row.getString(i))) // too large domain - use String instead
            else {
              val sv = row.getString(i)
              val smap = domHash(i)
              nchk.addEnum(smap.getOrElse(sv, !!!))
            }
          case _ => Double.NaN
        }
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

  private def collectColumnDomains(sc: SparkContext,
                                   rdd: SchemaRDD,
                                   fnames: Array[String],
                                   ftypes: Array[Class[_]]): Array[Array[String]] = {
    val res = Array.ofDim[Array[String]](fnames.length)
    for (idx <- 0 until ftypes.length if ftypes(idx).equals(classOf[String])) {
      val acc =  sc.accumulableCollection(new mutable.HashSet[String]())
      // Distributed ops
      rdd.foreach( r => { acc += r.getString(idx) })
      res(idx) = acc.value.toArray.sorted
    }
    res
  }

  private def collectColumnDomains[A <: Product](sc: SparkContext,
                                   rdd: RDD[A],
                                   fnames: Array[String],
                                   ftypes: Array[Class[_]]): Array[Array[String]] = {
    val res = Array.ofDim[Array[String]](fnames.length)
    for (idx <- 0 until ftypes.length if ftypes(idx).equals(classOf[String])) {
      val acc =  sc.accumulableCollection(new mutable.HashSet[String]())
      // Distributed ops
      rdd.foreach( r => { acc += r.productElement(idx).asInstanceOf[String] })
      res(idx) = acc.value.toArray.sorted
    }
    res
  }

  private def !!! = throw new IllegalArgumentException

  def toRDD[A <: Product: TypeTag: ClassTag]
           ( h2oContext : H2OContext, fr : DataFrame ) : RDD[A] = new H2ORDD[A](h2oContext,fr)
}
