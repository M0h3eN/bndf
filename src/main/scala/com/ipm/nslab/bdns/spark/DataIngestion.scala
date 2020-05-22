package com.ipm.nslab.bdns.spark

import scala.collection.JavaConverters._
import com.typesafe.scalalogging.Logger
import spray.json._
import com.ipm.nslab.bdns.commons.M2eeJsonProtocol.MapJsonFormat
import com.ipm.nslab.bdns.commons.{FileSystem, MongoConnector}
import com.ipm.nslab.bdns.evaluator.PathPropertiesEvaluator
import com.ipm.nslab.bdns.structure.SchemaCreator
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, round}
import us.hebi.matlab.mat.format.Mat5.readFromFile

class DataIngestion(MONGO_URI: String){

  val logger: Logger = Logger(s"${this.getClass.getName}")

  val MONGO_DB_NAME = "MetaDataDB"
  val MONGO_COLLECTION = "Experiments"

  val fileSystem = new FileSystem
  val schemaCreator = new SchemaCreator

  val RawFileFormat = "mat"
  val numberOfSqlPartition = 500
  val numberOfSlices = 500

  def writeExperimentMetaData(spark: SparkSession, rootDir: String): Unit ={
    import spark.implicits._
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    val rootPath = fileSystem.getRootPathProperties(rootDir)
    val rootPathDf = Seq(rootPath).toDF()
    mongoConnector.Writer(MONGO_COLLECTION, rootPathDf)

  }

  def writeAndCreateEvent(spark: SparkSession, rootDir: String, pathProperties: PathPropertiesEvaluator): Dataset[Row] ={
    import spark.implicits._
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    if(pathProperties.eventFileNames.isEmpty) logger.error("", throw new Exception("Event file not found"))

    val eventFullPath = pathProperties.eventFileNames.map(names => s"$rootDir/$names.mat")

    val eventDataSet: DataFrame = eventFullPath.map(ev => {

      val eventMatFile = readFromFile(ev)
      val eventEntry = eventMatFile.getEntries.asScala
      val eventFileName = fileSystem.getLeafFileName(ev)
      val eventFileHdfsWritePath = s"/${pathProperties.parentPath}/${pathProperties.experimentName}" +
        s"/events/$eventFileName.parquet"

      logger.info(s"Start Writing Event MetaData for $eventFileName")

      val samplingRate = schemaCreator.getValue(eventEntry, eventMatFile, "SamplingRate").toDouble
      val fileInfo = schemaCreator.getValue(eventEntry, eventMatFile, "long_name")

      val eventMetaData: Map[String, Any] = schemaCreator.metaDataSchemaCreator(eventEntry, eventMatFile)
      val eventMetaDatJson: Dataset[String] = spark.createDataset(Seq(eventMetaData.toJson.toString()))
      val eventMetaDatJsonDs: DataFrame = spark.read.json(eventMetaDatJson)
        .withColumn("_id", lit(eventFileName))
        .withColumn("FileInfo", lit(fileInfo))
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

      eventCompleteDs

    }).reduce((ds1, ds2) => ds1.unionByName(ds2))

    val eventHeadStart = eventDataSet.orderBy($"EventTime").select($"EventTime").first().getAs[Long](0)
    val FilteredEventDataSet = eventDataSet
      .withColumn("EventTime", $"EventTime" + eventHeadStart)
      .filter($"EventTime" > 0)
      .withColumn("EventTime", round((($"EventTime")/$"SamplingRate")/1000))
      .drop($"SamplingRate")

    FilteredEventDataSet

  }

  def writeAndCreateChannel(spark: SparkSession, eventDataSet: Dataset[Row], rootDir: String, pathProperties: PathPropertiesEvaluator): Dataset[Row]={
    import spark.implicits._
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    if(pathProperties.channelFileNames.isEmpty) logger.error("", throw new Exception("Channel file not found"))

    var channelTimeCounter: Long = 0

    val channelFullPath = pathProperties.channelFileNames.map(names => s"$rootDir/$names.mat")

    channelFullPath.map(channel => {

      val channelMatFile = readFromFile(channel)
      val channelEntry = channelMatFile.getEntries.asScala
      val channelFileName = fileSystem.getLeafFileName(channel)
      val channelFileHdfsWritePath = s"/${pathProperties.parentPath}/${pathProperties.experimentName}" +
        s"/channels/$channelFileName.parquet"

      logger.info(s"Start Writing Channel MetaData for $channelFileName")

      val fileInfo = schemaCreator.getValue(channelEntry, channelMatFile, "long_name")
      val channelMetaData = schemaCreator.metaDataSchemaCreator(channelEntry, channelMatFile)
      val channelMetaDatJson = spark.createDataset(Seq(channelMetaData.toJson.toString()))
      val channelMetaDatJsonDs = spark.read.json(channelMetaDatJson)
        .withColumn("_id", lit(channelFileName))
        .withColumn("FileInfo", lit(fileInfo))
        .withColumn("HDFS_PATH", lit(channelFileHdfsWritePath))

      mongoConnector.Writer(pathProperties.experimentName, channelMetaDatJsonDs)
      logger.info(s"Start Writing Event Data for $channelFileName")

      val channelData = schemaCreator.rawDataSchemaCreator(channelEntry, channelTimeCounter, channelMatFile).toSeq
      val channelDataPar = spark.sparkContext.parallelize(channelData, numberOfSlices)
      val channelDs = channelDataPar.toDS()

      val channelWithEventDs = channelDs
        .join(eventDataSet, $"Time" === $"EventTime", "left")
        .select($"Time", $"Signal", $"EventCode")

      channelWithEventDs.
        write
        .mode(SaveMode.Overwrite)
        .parquet(channelFileHdfsWritePath)

      channelTimeCounter = channelData.last.Time
      channelDs
        .withColumn("channelName", lit(fileSystem.getLeafFileName(channel)))
        .withColumn("FileInfo", lit(fileInfo))
    }).reduce((df1, df2) => df1.unionByName(df2))

  }
}
