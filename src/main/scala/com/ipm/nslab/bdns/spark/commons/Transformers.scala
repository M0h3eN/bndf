package com.ipm.nslab.bdns.spark.commons

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.WrappedArray

class Transformers {

  def arrayToVector(arr: WrappedArray[Double]) :Vector = Vectors.dense(arr.toArray)
  def arrayToVectorUDF = udf[Vector, WrappedArray[Double]](arrayToVector)

}
