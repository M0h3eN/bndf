package ir.ipm.bcol

import com.typesafe.scalalogging.Logger

import ir.ipm.bcol.commons.{MongoConnector, SparkConfig}
import ir.ipm.bcol.spark.DataIngestion


  /**
 * @author ${Mohsen Hadianpour}
 */
object RecordingDataLoader extends DataIngestion{

    override val logger: Logger = Logger(s"${this.getClass.getName}")

    val sparkConfig = new SparkConfig
    val dataIngestion = new DataIngestion

  def main(args : Array[String]) {

    if(args.isEmpty) logger.error("", throw new Exception("Path must specified"))

    val dir = args.apply(0)
    val conf = sparkConfig.sparkInitialConf("Recording Data Loader", MONGO_URI, MONGO_DB_NAME, numberOfSqlPartition)
    val spark = sparkConfig.sparkSessionCreator(conf)
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    val experimentsRootDirectory = fileSystem.getListOfDirs(dir)
    if (experimentsRootDirectory.isEmpty) logger.error("", throw new Exception("Directory is empty"))

    val writableExperiments = mongoConnector
      .checkIfItExist("Experiments", "fullPath", experimentsRootDirectory)
      .nonExistenceExperiments

    logger.info(s"Total number of ${writableExperiments.length} data directory found.")

    if (!writableExperiments.isEmpty){

    writableExperiments.foreach(ex => {

      logger.info(s"Start writing data for ${fileSystem.getLeafFileName(ex)} experiment")
      val pathProperties = fileSystem.getPathProperties(ex, RawFileFormat)

      val eventData = dataIngestion.writeAndCreateEvent(spark, ex, pathProperties)
      dataIngestion.writeChannel(spark, eventData.persist(), ex, pathProperties)

      dataIngestion.writeExperimentMetaData(spark, ex)

      eventData.unpersist()

    })

  } else {
      logger.warn("There is no new data to write")
    }
  }
}
