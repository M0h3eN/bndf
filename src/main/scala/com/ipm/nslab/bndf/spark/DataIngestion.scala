package com.ipm.nslab.bndf.spark

import scala.collection.JavaConverters._
import com.typesafe.scalalogging.Logger
import spray.json._
import com.ipm.nslab.bndf.commons.M2eeJsonProtocol.MapJsonFormat
import com.ipm.nslab.bndf.commons.io.SparkReader
import com.ipm.nslab.bndf.commons.{FileSystem, MongoConnector}
import com.ipm.nslab.bndf.extendedTypes.{ChannelCounterIterator, PathPropertiesEvaluator}
import com.ipm.nslab.bndf.structure.SchemaCreator
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, typedLit}
import us.hebi.matlab.mat.format.Mat5.readFromFile

import scala.util.Try

/** Considers all core functionality related to data ingestion
 * These features are mainly used by the RecordingDataLoader module
 * @param MONGO_URI MongoDB connection URL
 */
class DataIngestion(MONGO_URI: String){

  val logger: Logger = Logger(s"${this.getClass.getName}")

  val MONGO_DB_NAME = "MetaDataDB"
  val MONGO_COLLECTION = "Experiments"

  val fileSystem = new FileSystem
  val schemaCreator = new SchemaCreator
  val sparkReader = new SparkReader

  val RawFileFormat = "mat"
  val numberOfSqlPartition = 500
  val numberOfSlices = 500

  /** Provides meta-data write operation to MongoDB
   * @param spark Active spark session
   * @param rootDir The raw data root directory
   */
  def writeExperimentMetaData(spark: SparkSession, rootDir: String): Unit ={
    import spark.implicits._
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    val rootPath = fileSystem.getRootPathProperties(rootDir)
    val rootPathDf = Seq(rootPath).toDF()
    mongoConnector.Writer(MONGO_COLLECTION, rootPathDf)

  }

  /** Creates events dataset and write it in HDFS as the columnar parquet files
   * @param spark Active spark session
   * @param rootDir The raw data root directory
   * @param pathProperties The required information about raw files see PathPropertiesEvaluator case class
   * @return Event Dataset
   */
  def writeAndCreateEvent(spark: SparkSession, rootDir: String, pathProperties: PathPropertiesEvaluator): Dataset[Row] ={
    import spark.implicits._
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    if(pathProperties.eventFileNames.isEmpty) logger.error("", throw new Exception("Event file not found"))

    val eventFullPath = pathProperties.eventFileNames.map(names => s"$rootDir/$names.mat")

    eventFullPath.foreach(ev => {

      val eventMatFile = readFromFile(ev)
      val eventEntry = eventMatFile.getEntries.asScala
      val eventFileName = fileSystem.getLeafFileName(ev)
      val eventFileHdfsWritePath = s"/${pathProperties.parentPath}/${pathProperties.experimentName}" +
        s"/events/$eventFileName.parquet"

      logger.info(s"Start Writing Event MetaData for $eventFileName")

      val samplingRate = schemaCreator.getValue(eventEntry, eventMatFile, "SamplingRate").distinct.apply(0).toDouble
      val fileInfo = schemaCreator.getValue(eventEntry, eventMatFile, "long_name")
        .map(_.replace("'", ""))

      val eventMetaData: Map[String, Any] = schemaCreator.metaDataSchemaCreator(eventEntry, eventMatFile)
      val eventMetaDatJson: Dataset[String] = spark.createDataset(Seq(eventMetaData.toJson.toString().replace("'", "")))
      val eventMetaDatJsonDs: DataFrame = spark.read.json(eventMetaDatJson)
        .withColumn("_id", lit(eventFileName))
        .withColumn("FileInfo", typedLit[Array[String]](fileInfo))
        .withColumn("HDFS_PATH", lit(eventFileHdfsWritePath))
        .withColumn("IS_EVENT", lit(true))

      mongoConnector.Writer(pathProperties.experimentName, eventMetaDatJsonDs)
      logger.info(s"Start Writing Event Data for $eventFileName")

      val eventData = schemaCreator.eventDataSchemaCreator(eventEntry, eventMatFile).toSeq
      val eventDataPar = spark.sparkContext.parallelize(eventData, numberOfSlices)
      val eventDs = eventDataPar.toDS()

      val eventTimeDs = eventDs.filter($"Type" === "time").select("EventValue", "Id")
      val eventCondDs = eventDs.filter($"Type" === "code").select("EventValue", "Id")

      val eventCompleteDs = eventTimeDs.as("df1")
        .join(eventCondDs.as("df2"), Seq("Id"))
        .select($"df1.EventValue".as("EventTime"), $"df2.EventValue".as("EventCode"))
        .withColumn("SamplingRate", lit(samplingRate))

      eventCompleteDs
        .write
        .mode(SaveMode.Overwrite)
        .parquet(eventFileHdfsWritePath)

    })

    val eventFileHdfsWritePath = s"/${pathProperties.parentPath}/${pathProperties.experimentName}/events"
    val eventDataSet = sparkReader.eventParquetReader(spark, eventFileHdfsWritePath)
    val eventHeadStart = eventDataSet.orderBy($"EventTime").select($"EventTime").first().getAs[Long](0)
    val FilteredEventDataSet = eventDataSet
      .withColumn("EventTime", $"EventTime" + eventHeadStart)
      .filter($"EventTime" > 0)

    FilteredEventDataSet

  }

  /** Provides the channel data structure
   * Channel structure is created by matching constructed raw-data and event-data via Time index
   * @param spark Active spark session
   * @param eventDataSet Event Dataset
   * @param rootDir The raw data root directory
   * @param pathProperties The required information about raw files see PathPropertiesEvaluator case class
   */
  def writeAndCreateChannel(spark: SparkSession, eventDataSet: Dataset[Row], rootDir: String, pathProperties: PathPropertiesEvaluator): Unit={
    import spark.implicits._
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    if(pathProperties.channelFileNames.isEmpty) logger.error("", throw new Exception("Channel file not found"))

    var channelTimeCounter: Long = 0

    val channelFullPath = pathProperties.channelFileNames.map(names => s"$rootDir/$names.mat")
    val sortedIterator: Array[ChannelCounterIterator] = channelFullPath.par.map(channel => {

      val mainCounter = fileSystem.getChannelCounterInfo(channel).mainCounter
      val subCounter = fileSystem.getChannelCounterInfo(channel).subCounter

      val channelMatFile = readFromFile(channel)
      val channelEntry = channelMatFile.getEntries.asScala
      val channelFileName = fileSystem.getLeafFileName(channel)

      val channelFileInfo = schemaCreator.getValue(channelEntry, channelMatFile, "long_name")
        .filterNot(_.equalsIgnoreCase("Root"))
        .reduce((x, y) => {
          if(x.length > y.length) x.diff(y) else y.diff(x)
        })
        .replace("/", "")
        .replace("'", "")
        .asInstanceOf[String]

      ChannelCounterIterator(channel, channelFileName, channelFileInfo, mainCounter, subCounter)

    }).toArray.sortBy(cci => {
      (cci.channelFileInfo, cci.mainCounter, cci.subCounter)
    })

    sortedIterator.indices.foreach(ci => {

      val channelMatFile = readFromFile(sortedIterator.apply(ci).fullPath)
      val channelEntry = channelMatFile.getEntries.asScala
      val channelFileName = fileSystem.getLeafFileName(sortedIterator.apply(ci).channelFileName)
      val nextChannelFileName = Try(fileSystem.getLeafFileName(sortedIterator.apply(ci + 1).channelFileName))
        .getOrElse(fileSystem.getLeafFileName(sortedIterator.apply(0).channelFileName))
      val fileInfo = sortedIterator.apply(ci).channelFileInfo
      val nextFileInfo = Try(sortedIterator.apply(ci + 1).channelFileInfo).getOrElse(sortedIterator.apply(0).channelFileInfo)
      val currentChannelFileHdfsWritePath = s"/${pathProperties.parentPath}/${pathProperties.experimentName}" +
        s"/channels/$channelFileName.parquet"

      logger.info(s"Start Writing Channel MetaData for $channelFileName")

      val channelMetaData = schemaCreator.metaDataSchemaCreator(channelEntry, channelMatFile)
      val channelMetaDatJson = spark.createDataset(Seq(channelMetaData.toJson.toString().replace("'", "")))
      val channelMetaDatJsonDs = spark.read.json(channelMetaDatJson)
        .withColumn("_id", lit(channelFileName))
        .withColumn("FileInfo", lit(fileInfo))
        .withColumn("HDFS_PATH", lit(currentChannelFileHdfsWritePath))

      mongoConnector.Writer(pathProperties.experimentName, channelMetaDatJsonDs)
      logger.info(s"Start Writing Event Data for $channelFileName")

      val channelData = schemaCreator.rawDataSchemaCreator(channelEntry, channelTimeCounter, channelMatFile).toSeq
      val channelDataPar = spark.sparkContext.parallelize(channelData, numberOfSlices)
      val channelDs = channelDataPar.toDS()

      val channelWithEventDs = channelDs
        .join(eventDataSet, $"Time" === $"EventTime", "left")
        .select($"Time", $"Signal", $"EventCode")
        .withColumn("FileInfo", lit(fileInfo))

      channelWithEventDs.
        write
        .mode(SaveMode.Overwrite)
        .parquet(currentChannelFileHdfsWritePath)

      val current = channelFileName.split("channelik").apply(1).split("_").apply(0).toInt
      val next = nextChannelFileName.split("channelik").apply(1).split("_").apply(0).toInt
      val cond = current.equals(next) || fileInfo.equals(nextFileInfo)

      channelTimeCounter = if(cond) channelData.last.Time else 0

    })

  }
}
