package com.ipm.nslab.bndf

import java.util.concurrent.TimeUnit

import com.ipm.nslab.bndf.commons.{Benchmark, MongoConnector, SparkConfig}
import com.ipm.nslab.bndf.spark.DataIngestion
import com.typesafe.scalalogging.Logger

import scala.util.Try


  /** Implements Recording Data Loader module as a spark-submit job by initiating required fields and classes
 * Initiates Raw file path, spark configs, HDFS paths, Mongo connection
 */
object RecordingDataLoader extends DataIngestion(MONGO_URI = ""){

    override val logger: Logger = Logger(s"${this.getClass.getName}")

    private val sparkConfig: SparkConfig = new SparkConfig

    /** All operation related to RecordingDataLoader will start from this function
     * @param args It takes two argument from the user respectively
     *             1- Raw data path in the spark driver node
     *             2- A MongoDB URL connection for storing meta-data
     */
    def main(args : Array[String]) {

    if(args.isEmpty) logger.error("", throw new Exception("Path must specified"))

    val dir = args(0)
    val MONGO_URI_DEFAULT = "mongodb://root:ns123@mongos:27017/admin"
    val MONGO_URI = Try(args(1)) getOrElse(MONGO_URI_DEFAULT)
    val dataIngestion = new DataIngestion(MONGO_URI)
    val conf = sparkConfig.sparkInitialConf("Recording Data Loader", MONGO_URI, MONGO_DB_NAME, numberOfSqlPartition)
    val spark = sparkConfig.sparkSessionCreator(conf)
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)
    val benchmark = new Benchmark(spark, MONGO_URI, MONGO_DB_NAME)

    val experimentsRootDirectory = fileSystem.getListOfDirs(dir)
    if (experimentsRootDirectory.isEmpty) logger.error("", throw new Exception("Directory is empty"))

    val writableExperiments = mongoConnector
      .checkIfItExist("Experiments", "fullPath", experimentsRootDirectory)
      .nonExistenceExperiments

    if (!writableExperiments.isEmpty){
      logger.info(s"Total number of ${writableExperiments.length} data directory found.")

    writableExperiments.foreach(ex => {

      val startTime: Long = System.currentTimeMillis()
      logger.info(s"Start writing data for ${fileSystem.getLeafFileName(ex)} experiment")
      val pathProperties = fileSystem.getPathProperties(ex, RawFileFormat)

      val eventData = dataIngestion.writeAndCreateEvent(spark, ex, pathProperties)
      dataIngestion.writeAndCreateChannel(spark, eventData.persist(), ex, pathProperties)

      dataIngestion.writeExperimentMetaData(spark, ex)

      benchmark.WriteBenchmarkData(this.getClass.getName, ex,
        "CompletionOfWritingExperiment", System.currentTimeMillis() - startTime)

      eventData.unpersist()
    })

      logger.info("Finished Writing Data")
      TimeUnit.SECONDS.sleep(5)
      spark.close()

  } else {
      logger.warn("There is no new data to write")
      spark.close()
    }
  }
}
