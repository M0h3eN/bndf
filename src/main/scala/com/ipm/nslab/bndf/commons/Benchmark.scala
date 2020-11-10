package com.ipm.nslab.bndf.commons

import com.ipm.nslab.bndf.extendedTypes.BenchmarkDataSet
import org.apache.spark.sql.SparkSession

class Benchmark(override val spark: SparkSession, override val uri: String,
                         override val database: String)
  extends MongoConnector(spark: SparkSession,  uri: String, database: String){

  val sparkConfig = new SparkConfig

  def WriteBenchmarkData(moduleName: String, experimentName: String, stage: String, timeMil: Long): Unit = {
    import spark.implicits._

    val numberOfNodes = sparkConfig.currentActiveExecutorsCount(spark.sparkContext)
    val timeSecond = timeMil/1000D
    val timeMinutes = timeMil/(1000*60D)
    val _id = s"${moduleName}_${experimentName}_${numberOfNodes}_${stage}".hashCode

    val benchmarkData = BenchmarkDataSet(_id, moduleName, experimentName,
      numberOfNodes, stage, timeMinutes, timeSecond)
    val benchmarkDataFrame = Seq(benchmarkData).toDF()

    Writer("BENCHMARK", benchmarkDataFrame)
  }

}
