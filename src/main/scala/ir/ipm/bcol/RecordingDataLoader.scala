package ir.ipm.bcol

import com.typesafe.scalalogging.Logger

import scala.collection.JavaConverters._
import spray.json._
import ir.ipm.bcol.commons.M2eeJsonProtocol.MapJsonFormat
import ir.ipm.bcol.commons.{FileSystem, MongoConnector, SparkConfig}
import us.hebi.matlab.mat.format.Mat5.readFromFile
import ir.ipm.bcol.structure.SchemaCreator
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

/**
 * @author ${Mohsen Hadianpour}
 */
object RecordingDataLoader {

  val logger: Logger = Logger(s"${this.getClass.getName}")

  val MONGO_DB_NAME = "MetaDataDB"
  val MONGO_URI = "mongodb://root:ns123@mongos0:27017/admin"

  val sparkConfig = new SparkConfig
  val mongoConnector = new MongoConnector
  val fileSystem = new FileSystem

  def main(args : Array[String]) {

    val conf = sparkConfig.sparkInitialConf("Recording Data Loader", MONGO_URI, MONGO_DB_NAME)
    val spark = sparkConfig.sparkSessionCreator(conf)
    val schemaCreator = new SchemaCreator

    import spark.implicits._
    if(args.isEmpty) logger.error("Path Must specified", throw new Exception("Path Must specified"))

    val RawFileFormat = "mat"
    val fullPath = fileSystem.getListOfFiles(args.apply(0), RawFileFormat)

    val pathLen = fullPath.map(x => x.split("/")).head.length
    val parrentPath = fullPath.map(x => x.split("/").apply(pathLen-3)).head
    val experimentName = fullPath.map(x => x.split("/").apply(pathLen-2)).head
    val channelFileNames = fullPath.map(x => x.split("/").apply(pathLen-1).split("\\.").apply(0))
      .filter(c => c.startsWith("channel")).sorted
    val eventFileNames = fullPath.map(x => x.split("/").apply(pathLen-1).split("\\.").apply(0))
      .filter(c => c.startsWith("event")).sorted

    if(eventFileNames.isEmpty) logger.error("Event file not found", throw new Exception("Event file not found"))

    val eventFullPath = fullPath.filter(x => eventFileNames.map(y => x.contains(y)).reduce(_ || _)).sorted
    val channelFullPath = fullPath.filter(x => channelFileNames.map(y => x.contains(y)).reduce(_ || _)).sorted

    eventFullPath.foreach(println)

    val eventDataSet = eventFullPath.map(ev => {

      val eventMatFile = readFromFile(ev)
      val eventEntry = eventMatFile.getEntries.asScala
      val eventFileName = ev.split("/").apply(pathLen-1).split("\\.").apply(0)

      logger.info(s"Start Writing Event MetaData for $eventFileName")

      val eventFileHdfsWritePath = s"/$parrentPath/$experimentName/$eventFileName.parquet"
      val eventMetaData = schemaCreator.metaDataSchemaCreator(eventEntry, eventMatFile)
      val eventMetaDatJson = spark.createDataset(Seq(eventMetaData.toJson.toString()))
      val eventMetaDatJsonDs = spark.read.json(eventMetaDatJson).withColumn("_id", lit(eventFileName))

      mongoConnector.Writer(spark, eventMetaDatJsonDs, MONGO_URI, MONGO_DB_NAME, experimentName)
      logger.info(s"Start Writing Event Data for $eventFileName")

      val eventData = schemaCreator.eventDataSchemaCreator(eventEntry, eventMatFile)
      val eventDs = eventData.toSeq.toDS()

      val eventTimeDs = eventDs.filter($"Type" === "time").select("EventValue", "Id")
      val eventCondDs = eventDs.filter($"Type" === "code").select("EventValue", "Id")

      val eventCompleteDs = eventTimeDs.as("df1")
          .join(eventCondDs.as("df2"), Seq("Id"))
          .select($"df1.EventValue".as("EventTime"), $"df2.EventValue".as("EventCode"))

      eventCompleteDs
        .write
        .mode(SaveMode.Overwrite)
        .parquet(eventFileHdfsWritePath)

      eventCompleteDs

    }).reduce((ds1, ds2) => ds1.union(ds2))

    val eventHeadStart = eventDataSet.select($"EventTime").first().getAs[Long](0)
    val FilteredEventDataSet = eventDataSet
      .withColumn("EventTime", $"EventTime" + eventHeadStart)
      .filter($"EventTime" > 0)



  }

}
