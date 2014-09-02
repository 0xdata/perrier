package org.apache.spark.rdd

import org.apache.spark.h2o.{ReflectionUtils, H2OContext}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import water.fvec.DataFrame
import water.{DKV, Key}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Convert DataFrame into an RDD (lazily)
 */

private[spark]
class H2ORDD[A <: Product: TypeTag: ClassTag] private (@transient sc: SparkContext, @transient fr: DataFrame, val colNames: Array[String]) extends RDD[A](sc, Nil) {

  // Get column names before building an RDD
  def this(sc: SparkContext, fr : DataFrame ) = this(sc,fr,ReflectionUtils.names[A])
  // Cache a way to get DataFrame from the K/V
  val keyName = fr._key.toString
  // Check that DataFrame & given Scala type are compatible
  colNames.foreach{ name => {
    if (fr.find(name) == -1) throw new IllegalArgumentException("Scala type has field " + name + " but DataFrame does not have a matching column; has "+fr._names.mkString(","))
  }}
  @transient val jc = implicitly[ClassTag[A]].runtimeClass
  @transient val cs = jc.getConstructors
  if( cs.length != 1 ) throw new IllegalArgumentException("Class "+typeTag[A]+" does not have exactly 1 constructor, I do not know which one to use")
  @transient val ccr = cs(0)
  if( ccr.getParameterTypes.length != colNames.length ) throw new IllegalArgumentException("Constructor must take exactly "+colNames.length+" args")

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    new Iterator[A] {
      val fr : DataFrame = DKV.get(Key.make(keyName)).get.asInstanceOf[DataFrame]

      val jc = implicitly[ClassTag[A]].runtimeClass
      val cs = jc.getConstructors
      val ccr = cs(0)

      val chks = fr.getChunks(split.index)
      val nrows = chks(0).len
      var row : Int = 0
      def hasNext: Boolean = row < nrows
      def next(): A = {
        val data : Array[Option[Any]] = for( chk <- chks ) yield if( chk.isNA0(row) ) None else Some(chk.at0(row))
        row += 1
        ccr.newInstance(data:_*).asInstanceOf[A]
      }
    }
  }

  /** Pass thru an RDD if given one, else pull from the H2O Frame */
  override protected def getPartitions: Array[Partition] = {
    val num = fr.anyVec().nChunks()
    val res = new Array[Partition](num)
    for( i <- 0 until num ) res(i) = new Partition { val index = i }
    res
  }

}
