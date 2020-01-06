package ir.ipm.bcol

import scala.collection.JavaConverters._
import spray.json._
import ir.ipm.bcol.commons.M2eeJsonProtocol.MapJsonFormat
import ir.ipm.bcol.commons.SparkConfig
import us.hebi.matlab.mat.format.Mat5.readFromFile
import ir.ipm.bcol.structure.SchemaCreator
import ir.ipm.bcol.commons.MongoConnector

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

/**
 * @author ${Mohsen Hadianpour}
 */
object RecordingDataLoader {

  val MONGO_DB_NAME = "MetaDataDB"
  val MONGO_URI = "mongodb://root:ns123@mongos0:27017/admin"

  val sparkConfig = new SparkConfig
  val mongoConnector = new MongoConnector

  def main(args : Array[String]) {

    val conf = sparkConfig.sparkInitialConf("Recording Data Loader", MONGO_URI, MONGO_DB_NAME)
    val spark = sparkConfig.sparkSessionCreator(conf)
    val schemaObject = new SchemaCreator

    import spark.implicits._

    val filePath = "/data/raw-data/Experiment_Kopo_2018-04-25_J9_8600/"
    val experiment = "Experiment_Kopo_2018-04-25_J9_8600"
    val hdfsWritePath = "/" + experiment + "/"
    val fileName = "channelik0_1.mat"

    val matTestFile = readFromFile(filePath + fileName)
    val entries = matTestFile.getEntries.asScala

    val metaData = schemaObject.metaDataSchemaCreator(entries, matTestFile)
    val rawData = schemaObject.dataSchemaCreator(entries, matTestFile)

    val metaDataJson = Seq(metaData.toJson.toString())
    val metaDataJsonDataSet = spark.createDataset(metaDataJson)
    val metaDataDf = spark.read.json(metaDataJsonDataSet).withColumn("_id", lit(fileName.split("\\.").apply(0)))
    val dataDF = rawData.toSeq.toDF()


    mongoConnector.Writer(spark, metaDataDf, MONGO_URI, MONGO_DB_NAME, experiment)

    dataDF
      .write
      .mode(SaveMode.Overwrite)
      .parquet(hdfsWritePath + fileName.split("\\.").apply(0) + ".parquet")

    println(dataDF.count())


  }

}
