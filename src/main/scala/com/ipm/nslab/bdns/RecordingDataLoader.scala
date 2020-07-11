package com.ipm.nslab.bdns

import java.util.concurrent.TimeUnit

import com.ipm.nslab.bdns.commons.{MongoConnector, SparkConfig}
import com.ipm.nslab.bdns.spark.DataIngestion
import com.typesafe.scalalogging.Logger

import scala.util.Try


  /**
 * @author ${Mohsen Hadianpour}
 */
object RecordingDataLoader extends DataIngestion(MONGO_URI = ""){

    override val logger: Logger = Logger(s"${this.getClass.getName}")

    val sparkConfig = new SparkConfig

  def main(args : Array[String]) {

    if(args.isEmpty) logger.error("", throw new Exception("Path must specified"))

    val dir = args.apply(0)
    val MONGO_URI_DEFAULT = "mongodb://root:ns123@mongos:27017/admin"
    val MONGO_URI = Try(args.apply(1)) getOrElse(MONGO_URI_DEFAULT)
    val dataIngestion = new DataIngestion(MONGO_URI)
    val conf = sparkConfig.sparkInitialConf("Recording Data Loader", MONGO_URI, MONGO_DB_NAME, numberOfSqlPartition)
    val spark = sparkConfig.sparkSessionCreator(conf)
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    val experimentsRootDirectory = fileSystem.getListOfDirs(dir)
    if (experimentsRootDirectory.isEmpty) logger.error("", throw new Exception("Directory is empty"))

    val writableExperiments = mongoConnector
      .checkIfItExist("Experiments", "fullPath", experimentsRootDirectory)
      .nonExistenceExperiments

    if (!writableExperiments.isEmpty){
      logger.info(s"Total number of ${writableExperiments.length} data directory found.")

    writableExperiments.foreach(ex => {

      logger.info(s"Start writing data for ${fileSystem.getLeafFileName(ex)} experiment")
      val pathProperties = fileSystem.getPathProperties(ex, RawFileFormat)

      val eventData = dataIngestion.writeAndCreateEvent(spark, ex, pathProperties)
      dataIngestion.writeAndCreateChannel(spark, eventData.persist(), ex, pathProperties)

      dataIngestion.writeExperimentMetaData(spark, ex)

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
