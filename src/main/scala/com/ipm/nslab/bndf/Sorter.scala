package com.ipm.nslab.bndf

import java.util.concurrent.TimeUnit

import com.ipm.nslab.bndf.commons.io.{HdfsOperator, SparkWriter}
import com.ipm.nslab.bndf.commons.{Benchmark, MongoConnector, SparkConfig}
import com.ipm.nslab.bndf.spark.analysis.Sorting
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SaveMode

import scala.util.Try

/** Implement Spike Sorter Module as a spark-submit job by initiating required fields and classes
 * Initiates spark configs, HDFS paths, Mongo connection to access meta-data
 */
object Sorter {

  val logger: Logger = Logger(s"${this.getClass.getName}")

  private val sparkConfig: SparkConfig = new SparkConfig
  private val sparkWrite: SparkWriter = new SparkWriter

  private val hdfsOperator: HdfsOperator = new HdfsOperator

  private val HIVE_DB: String = "sortedDataDB"

  private val SPARK_CHECKPOINT_DIR: String = "/spark/checkpoint_bdns"
  private val MAIN_SORTED_DATA_DIR: String = "/sorted-data"

  private val MONGO_DB_NAME: String = "MetaDataDB"
  private val MONGO_COLLECTION: String = "Experiments"

  private val numberOfSqlPartition: Int = 500

  /** All operation related to sorting will start from this function
   * @param args It takes two argument from the user respectively
   *             1- A MongoDB URL connection of stored meta-data
   *             2- The Experiment(Session) name for sorting procedure
   *             If the argument 2 is not specified by the user, Sorter will process all available Experiments(Sessions).
   */
  def main(args: Array[String]): Unit = {

    if(args.isEmpty) logger.error("", throw new Exception("Experiment name must specified"))

    val MONGO_URI_DEFAULT = "mongodb://root:ns123@mongos:27017/admin"
    val MONGO_URI = Try(args(0)).getOrElse(MONGO_URI_DEFAULT)
    val sorting = new Sorting
    val conf = sparkConfig.sparkInitialConf("Spike Sorter", MONGO_URI, MONGO_DB_NAME, numberOfSqlPartition)
    val spark = sparkConfig.sparkSessionCreator(conf)
    spark.sparkContext.setCheckpointDir(SPARK_CHECKPOINT_DIR)

    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)
    val benchmark = new Benchmark(spark, MONGO_URI, MONGO_DB_NAME)

    if (!spark.catalog.databaseExists(HIVE_DB)) spark.sql(s"CREATE DATABASE $HIVE_DB")

    val exList = mongoConnector.Reader(MONGO_COLLECTION).select("_id").collect().map(_.getAs[String](0))
    val ex = Try(args(1))

    if(ex.isSuccess) {
      val metaData = mongoConnector.Reader(ex.get)
      val startTime = System.currentTimeMillis()
      val sortedDataset = sorting.sorter(spark, metaData, ex.get)
      benchmark.WriteBenchmarkData(this.getClass.getName, ex.get,
        "CompletionOfExperimentSorting", System.currentTimeMillis() - startTime)

      val startWTime = System.currentTimeMillis()
      sparkWrite.writeParquetInHdfs(sortedDataset, SaveMode.Overwrite, MAIN_SORTED_DATA_DIR + "/" + ex + ".parquet")
      benchmark.WriteBenchmarkData(this.getClass.getName, ex.get,
        "CompletionOfWritingSortedData", System.currentTimeMillis() - startWTime)

      logger.info("Finished Sorting Spike Trains")
      hdfsOperator.deleteFileBasedOnDir(SPARK_CHECKPOINT_DIR)
      TimeUnit.SECONDS.sleep(5)
      spark.close()

    } else {
      exList.foreach(ex => {

        val metaData = mongoConnector.Reader(ex)
        val startTime = System.currentTimeMillis()
        val sortedDataset = sorting.sorter(spark, metaData, ex)
        benchmark.WriteBenchmarkData(this.getClass.getName, ex,
          "CompletionOfExperimentSorting", System.currentTimeMillis() - startTime)

        val startWTime = System.currentTimeMillis()
        sparkWrite.writeParquetInHdfs(sortedDataset, SaveMode.Overwrite, MAIN_SORTED_DATA_DIR + "/" + ex + ".parquet")
        benchmark.WriteBenchmarkData(this.getClass.getName, ex,
          "CompletionOfWritingSortedData", System.currentTimeMillis() - startWTime)

        logger.info(s"Finished Sorting Spike Trains of experiment $ex")

        spark.catalog.clearCache()

      })

      hdfsOperator.deleteFileBasedOnDir(SPARK_CHECKPOINT_DIR)
      TimeUnit.SECONDS.sleep(5)
      spark.close()
    }

  }
}
