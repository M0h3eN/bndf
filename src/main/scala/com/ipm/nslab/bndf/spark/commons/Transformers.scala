package com.ipm.nslab.bndf.spark.commons

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

/** Provides transformation required by passing data to different spark modules
 * For example, arrayToVector transform sparkSql modules arrays to sparkML Vector type
 */
class Transformers {

  val arrayToVector: mutable.WrappedArray[Double] => Vector = (arr: mutable.WrappedArray[Double]) => Vectors.dense(arr.toArray)
  val arrayToVectorUDF: UserDefinedFunction = udf[Vector, mutable.WrappedArray[Double]](arrayToVector)

}
