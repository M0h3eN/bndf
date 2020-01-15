package ir.ipm.bcol

import com.typesafe.scalalogging.Logger
import ir.ipm.bcol.commons.{MongoConnector, SparkConfig}
import ir.ipm.bcol.spark.DataIngestion
import org.apache.spark.sql.SaveMode


  /**
 * @author ${Mohsen Hadianpour}
 */
object RecordingDataLoader extends DataIngestion{

    override val logger: Logger = Logger(s"${this.getClass.getName}")

    val sparkConfig = new SparkConfig
    val dataIngestion = new DataIngestion
    val HIVE_DB = "rawDataDB"

  def main(args : Array[String]) {

    if(args.isEmpty) logger.error("", throw new Exception("Path must specified"))

    val dir = args.apply(0)
    val conf = sparkConfig.sparkInitialConf("Recording Data Loader", MONGO_URI, MONGO_DB_NAME, numberOfSqlPartition)
    val spark = sparkConfig.sparkSessionCreator(conf)
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    if (!spark.catalog.databaseExists(HIVE_DB)) spark.sql(s"CREATE DATABASE $HIVE_DB")

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
      val channelData = dataIngestion.writeAndCreateChannel(spark, eventData.persist(), ex, pathProperties).persist()

      channelData.write.mode(SaveMode.Overwrite)
        .saveAsTable(HIVE_DB + "." + fileSystem.getLeafFileName(ex).replace("-", "_"))
      dataIngestion.writeExperimentMetaData(spark, ex)

      eventData.unpersist()
      channelData.unpersist()

    })

  } else {
      logger.warn("There is no new data to write")
    }
  }
}
