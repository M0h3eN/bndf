package com.ipm.nslab.bndf.commons

import com.ipm.nslab.bndf.extendedTypes.BenchmarkDataSet
import org.apache.spark.sql.SparkSession

/** Provides configuration related to benchmarking and logging
 * @param spark    An active spark session
 * @param uri      MongoDB URL
 * @param database MongoDB database name
 */
class Benchmark(override val spark: SparkSession, override val uri: String,
                         override val database: String)
  extends MongoConnector(spark: SparkSession,  uri: String, database: String){

  val sparkConfig = new SparkConfig

  /** Writes Benchmark to MongoDB
   * @param moduleName The name of the used module
   * @param experimentName The name of the experiment
   * @param stage The stage of the module operation
   * @param timeMil Total elapsed time in milliseconds
   */
  def WriteBenchmarkData(moduleName: String, experimentName: String, stage: String, timeMil: Long): Unit = {
    import spark.implicits._

    val numberOfNodes = sparkConfig.currentActiveExecutorsCount(spark.sparkContext)
    val timeSecond = timeMil/1000D
    val timeMinutes = timeMil/(1000*60D)
    val _id = s"${moduleName}_${experimentName}_${numberOfNodes}_$stage".hashCode

    val benchmarkData = BenchmarkDataSet(_id, moduleName, experimentName,
      numberOfNodes, stage, timeMinutes, timeSecond)
    val benchmarkDataFrame = Seq(benchmarkData).toDF()

    Writer("BENCHMARK", benchmarkDataFrame)
  }

}
