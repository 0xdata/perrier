package org.apache.spark.h2o

import org.apache.spark.sql.catalyst.types.{BinaryType, DoubleType, IntegerType, DataType, FloatType}
import java.io.File
import water.fvec.DataFrame
import water.Key
import org.apache.spark.sql.{Row, SchemaRDD}
import scala.language.implicitConversions
import org.apache.spark.{TaskContext, SparkContext}
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.rdd.{H2ORDD, RDD}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Simple H2O context motivated by SQLContext.
 *
 * Doing - implicit conversion from RDD -> H2OLikeRDD
 */
@AlphaComponent
class H2OContext(@transient val sparkContext: SparkContext)
  extends org.apache.spark.Logging
  with H2OConf
  with Serializable {

//private def perPartition[A: ClassTag]( h2ordd: H2ORDD[A] ) ( context: TaskContext, it: Iterator[A] ): (Int,Long) = {
//  // An array of H2O NewChunks; A place to record all the data in this partition
//  val nchks = water.fvec.Frame.createNewChunks(h2ordd.h2oName,context.partitionId)
//  it.foreach(row => {                    // For all rows...
//    val prod = row.asInstanceOf[Product] // Easy access to all fields
//    for( i <- 0 until prod.productArity ) { // For all fields...
//      val fld = prod.productElement(i)
//      nchks(i).addNum({                  // Copy numeric data from fields to NewChunks
//        val x = fld match { case Some(n) => n; case _ => fld }
//        x match {
//          case n: Number => n.doubleValue
//          case n: Boolean => if (n) 1 else 0
//          case _ => Double.NaN
//        }
//      })
//    }
//  })
//  // Compress & write out the Partition/Chunks
//  water.fvec.Frame.closeNewChunks(nchks)
//  // Return Partition# and rows in this Partition
//  (context.partitionId,nchks(0).len)
//}
//
//private def perSQLPartition( h2ordd: H2ORDD[_], types: Array[Class[_]] ) ( context: TaskContext, it: Iterator[Row] ): (Int,Long) = {
//  val nchks = water.fvec.Frame.createNewChunks(h2ordd.h2oName,context.partitionId)
//  it.foreach(row => {
//    for( i <- 0 until h2ordd.colNames.length) {
//      nchks(i).addNum(
//        if (row.isNullAt(i)) Double.NaN
//        else types(i) match {
//          case q if q == classOf[Integer] => row.getInt(i)
//          case q if q == classOf[java.lang.Float] => row.getFloat(i)
//          case q if q == classOf[java.lang.Double] => row.getDouble(i)
//          case q if q == classOf[Boolean] => if( row.getBoolean(i)) 1 else 0
//          case uet => throw new IllegalArgumentException(s"Unexpected type $uet")
//        }
//      )
//    }
//  })
//  // Compress & write out the Partition/Chunks
//  water.fvec.Frame.closeNewChunks(nchks)
//  // Return Partition# and rows in this Partition
//  (context.partitionId,nchks(0).len)
//}
//
//private def dataTypeToClass(dt : DataType):Class[_] = dt match {
//  case BinaryType  => classOf[Integer]
//  case IntegerType => classOf[Integer]
//  case FloatType   => classOf[java.lang.Float]
//  case DoubleType  => classOf[java.lang.Double]
//  case _ => throw new IllegalArgumentException(s"Unsupported type $dt")
//}
//
//// Returns an H2ORDD as a side-effect makes an H2O Frame with the RDD data.
//// The input RDD is eagerly evaluated
//def createH2ORDD[A: TypeTag: ClassTag](rdd: RDD[A], name: String): H2ORDD[A] = {
//  // Pull out the RDD members; the names become H2O column names; the type guide conversion
//  val tt = typeOf[A].members.sorted.filter(!_.isMethod).toArray
//  val names = tt.map(_.name.toString.trim)
//
//  // Make an H2ORDD, and within it an H2O data Frame - but with no backing data (yet)
//  val h2ordd = new H2ORDD(name, names, this, this.sparkContext, rdd, null)
//  h2ordd.fr.preparePartialFrame(names)
//
//  // Parallel-fill the H2O data Chunks
//  val rows = sparkContext.runJob(h2ordd, perPartition[A](h2ordd) _) // eager, not lazy, evaluation
//  val res = new Array[Long](rdd.partitions.size)
//  rows.foreach{ case(cidx,nrows) => res(cidx) = nrows }
//
//  // Add Vec headers per-Chunk, and finalize the H2O Frame
//  h2ordd.fr.finalizePartialFrame(res)
//  h2ordd
//}
//
//// Returns an H2ORDD referring to an H2O Frame built from the given filename
//def parse[A: ClassTag]( name : String, f : File ) = {
//  val fr : water.fvec.Frame = water.util.FrameUtils.parseFrame(water.Key.make(name), f)
//  new H2ORDD[A](name, fr.names(), this, this.sparkContext, null, fr)
//}
}

object H2OContext {

  def colsFromType[A: TypeTag]() : Array[String] = {
    // Pull out the RDD members; the names become H2O column names; the type guide conversion
    val tt = typeOf[A].members.sorted.filter(!_.isMethod).toArray
    tt.map(_.name.toString.trim)
  }

  def toDataFrame(rdd: SchemaRDD) : DataFrame = {
    val names = rdd.schema.fieldNames.toArray
    val types = rdd.schema.fields.map( field => dataTypeToClass(field.dataType) ).toArray

    // Make an H2O data Frame - but with no backing data (yet)
    val key = Key.rand
    val fr = new water.fvec.Frame(key)
    fr.preparePartialFrame(names)

    // Extend the SchemaRDD to pass along the frame name

    //val rows = sparkContext.runJob(h2ordd, perSQLPartition(h2ordd, types) _) // eager, not lazy, evaluation
    //val res = new Array[Long](rdd.partitions.size)
    //rows.foreach{ case(cidx,nrows) => res(cidx) = nrows }
    //  
    //// Add Vec headers per-Chunk, and finalize the H2O Frame
    //h2ordd.fr.get.finalizePartialFrame(res)
    //h2ordd
    ???
  }

  private def dataTypeToClass(dt : DataType):Class[_] = dt match {
    case BinaryType  => classOf[Integer]
    case IntegerType => classOf[Integer]
    case FloatType   => classOf[java.lang.Float]
    case DoubleType  => classOf[java.lang.Double]
    case _ => throw new IllegalArgumentException(s"Unsupported type $dt")
  }

  //private def perSQLPartition( h2ordd: H2ORDD[_], types: Array[Class[_]] ) ( context: TaskContext, it: Iterator[Row] ): (Int,Long) = {
  //  val nchks = water.fvec.Frame.createNewChunks(h2ordd.h2oName,context.partitionId)
  //  it.foreach(row => {
  //    for( i <- 0 until h2ordd.colNames.length) {
  //      nchks(i).addNum(
  //        if (row.isNullAt(i)) Double.NaN
  //        else types(i) match {
  //          case q if q == classOf[Integer] => row.getInt(i)
  //          case q if q == classOf[java.lang.Float] => row.getFloat(i)
  //          case q if q == classOf[java.lang.Double] => row.getDouble(i)
  //          case q if q == classOf[Boolean] => if( row.getBoolean(i)) 1 else 0
  //          case uet => throw new IllegalArgumentException(s"Unexpected type $uet")
  //        }
  //      )
  //    }
  //  })
  //  // Compress & write out the Partition/Chunks
  //  water.fvec.Frame.closeNewChunks(nchks)
  //  // Return Partition# and rows in this Partition
  //}

  def toRDD[A: TypeTag: ClassTag]( sc : SparkContext, fr : DataFrame ) : RDD[A] = new H2ORDD[A](sc,fr)
}
