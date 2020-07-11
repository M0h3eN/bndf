package com.ipm.nslab.bdns.spark.commons

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

class Transformers {

  val arrayToVector: mutable.WrappedArray[Double] => Vector = (arr: mutable.WrappedArray[Double]) => Vectors.dense(arr.toArray)
  val arrayToVectorUDF: UserDefinedFunction = udf[Vector, mutable.WrappedArray[Double]](arrayToVector)

}
