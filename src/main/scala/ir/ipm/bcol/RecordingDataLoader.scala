package ir.ipm.bcol

import com.typesafe.scalalogging.Logger

import scala.collection.JavaConverters._
import spray.json._
import ir.ipm.bcol.commons.M2eeJsonProtocol.MapJsonFormat
import ir.ipm.bcol.commons.{FileSystem, MongoConnector, SparkConfig}
import us.hebi.matlab.mat.format.Mat5.readFromFile
import ir.ipm.bcol.structure.SchemaCreator
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.lit


  /**
 * @author ${Mohsen Hadianpour}
 */
object RecordingDataLoader {

  val logger: Logger = Logger(s"${this.getClass.getName}")

  val MONGO_DB_NAME = "MetaDataDB"
  val MONGO_URI = "mongodb://root:ns123@mongos:27017/admin"
  val sparkConfig = new SparkConfig
  val mongoConnector = new MongoConnector
  val fileSystem = new FileSystem

  val numberOfSqlPartition = 500
  val numberOfSlices = 500

  def main(args : Array[String]) {

    val conf = sparkConfig.sparkInitialConf("Recording Data Loader", MONGO_URI, MONGO_DB_NAME, numberOfSqlPartition)
    val spark = sparkConfig.sparkSessionCreator(conf)
    val schemaCreator = new SchemaCreator

    import spark.implicits._
    if(args.isEmpty) logger.error("Path Must specified", throw new Exception("Path Must specified"))

    val RawFileFormat = "mat"
    val filePaths = args.apply(0)
    val fullPath = fileSystem.getListOfFiles(args.apply(0), RawFileFormat)

    val pathLen = fullPath.map(x => x.split("/")).head.length
    val parentPath = fullPath.map(x => x.split("/").apply(pathLen-3)).head
    val experimentName = fullPath.map(x => x.split("/").apply(pathLen-2)).head
    val channelFileNames = fullPath.map(x => x.split("/").apply(pathLen-1).split("\\.").apply(0))
      .filter(c => c.startsWith("channel"))
      .sortBy(x => x.splitAt("channelik".length)._2.split("_").apply(0).toInt)
    val eventFileNames = fullPath.map(x => x.split("/").apply(pathLen-1).split("\\.").apply(0))
      .filter(c => c.startsWith("event")).sorted

    if(eventFileNames.isEmpty) logger.error("Event file not found", throw new Exception("Event file not found"))

    val eventFullPath = eventFileNames.map(names => s"$filePaths/$names.mat")
    val channelFullPath = channelFileNames.map(names => s"$filePaths/$names.mat")

    val eventDataSet: DataFrame = eventFullPath.map(ev => {

      val eventMatFile = readFromFile(ev)
      val eventEntry = eventMatFile.getEntries.asScala
      val eventFileName = ev.split("/").apply(pathLen-1).split("\\.").apply(0)

      logger.info(s"Start Writing Event MetaData for $eventFileName")

      val eventFileHdfsWritePath = s"/$parentPath/$experimentName/$eventFileName.parquet"
      val eventMetaData = schemaCreator.metaDataSchemaCreator(eventEntry, eventMatFile)
      val eventMetaDatJson = spark.createDataset(Seq(eventMetaData.toJson.toString()))
      val eventMetaDatJsonDs = spark.read.json(eventMetaDatJson).withColumn("_id", lit(eventFileName))

      mongoConnector.Writer(spark, eventMetaDatJsonDs, MONGO_URI, MONGO_DB_NAME, experimentName)
      logger.info(s"Start Writing Event Data for $eventFileName")

      val eventData = schemaCreator.eventDataSchemaCreator(eventEntry, eventMatFile).toSeq
      val eventDataPar = spark.sparkContext.parallelize(eventData, numberOfSlices)
      val eventDs = eventDataPar.toDS()

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
      .persist()

    var channelTimeCounter: Long = 0

    channelFullPath.foreach(channel => {

      val channelMatFile = readFromFile(channel)
      val channelEntry = channelMatFile.getEntries.asScala
      val channelFileName = channel.split("/").apply(pathLen-1).split("\\.").apply(0)

      logger.info(s"Start Writing Channel MetaData for $channelFileName")

      val channelFileHdfsWritePath = s"/$parentPath/$experimentName/$channelFileName.parquet"
      val channelMetaData = schemaCreator.metaDataSchemaCreator(channelEntry, channelMatFile)
      val channelMetaDatJson = spark.createDataset(Seq(channelMetaData.toJson.toString()))
      val channelMetaDatJsonDs = spark.read.json(channelMetaDatJson).withColumn("_id", lit(channelFileName))

      mongoConnector.Writer(spark, channelMetaDatJsonDs, MONGO_URI, MONGO_DB_NAME, experimentName)
      logger.info(s"Start Writing Event Data for $channelFileName")

      val channelData = schemaCreator.rawDataSchemaCreator(channelEntry, channelTimeCounter, channelMatFile).toSeq
      val channelDataPar = spark.sparkContext.parallelize(channelData, numberOfSlices)
      val channelDs = channelDataPar.toDS()

      val channelWithEventDs = channelDs
        .join(FilteredEventDataSet, $"Time" === $"EventTime", "left")
        .select($"Time", $"Signal", $"EventCode")

      channelWithEventDs.
        write
        .mode(SaveMode.Overwrite)
        .parquet(channelFileHdfsWritePath)

      channelTimeCounter = channelData.last.Time

    })

    FilteredEventDataSet.unpersist()

  }

}
