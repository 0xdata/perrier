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

/**
 * Work with reflection only inside this helper.
 */
object ReflectionUtils {
  import scala.reflect.runtime.universe._

  def names[T: TypeTag] : Array[String] = {
    val tt = typeOf[T].members.sorted.filter(!_.isMethod).toArray
    tt.map(_.name.toString.trim)
  }

  def types[T: TypeTag] : Array[Class[_]] = {
    val tt = typeOf[T].members.sorted.filter(!_.isMethod).toArray
    tt.map( _.typeSignature match {
      case t if t <:< typeOf[String]            => classOf[String]
      case t if t <:< typeOf[java.lang.Integer] => classOf[java.lang.Integer]
      case t if t <:< typeOf[java.lang.Long]    => classOf[java.lang.Long]
      case t if t <:< typeOf[java.lang.Double]  => classOf[java.lang.Double]
      case t if t <:< typeOf[java.lang.Float]   => classOf[java.lang.Float]
      case t if t <:< typeOf[java.lang.Short]   => classOf[java.lang.Short]
      case t if t <:< typeOf[java.lang.Byte]    => classOf[java.lang.Byte]
      case t if t <:< typeOf[java.lang.Boolean] => classOf[java.lang.Boolean]
      case t if t <:< definitions.IntTpe        => classOf[java.lang.Integer]
      case t if t <:< definitions.LongTpe       => classOf[java.lang.Long]
      case t if t <:< definitions.DoubleTpe     => classOf[java.lang.Double]
      case t if t <:< definitions.FloatTpe      => classOf[java.lang.Float]
      case t if t <:< definitions.ShortTpe      => classOf[java.lang.Short]
      case t if t <:< definitions.ByteTpe       => classOf[java.lang.Byte]
      case t if t <:< definitions.BooleanTpe    => classOf[java.lang.Boolean]
      case t => throw new IllegalArgumentException(s"Type $t is not supported!")
    })
  }
}
