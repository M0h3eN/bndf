package ir.ipm.bcol.spark

import scala.collection.JavaConverters._
import com.typesafe.scalalogging.Logger
import ir.ipm.bcol.commons.{FileSystem, MongoConnector}
import spray.json._
import ir.ipm.bcol.commons.M2eeJsonProtocol.MapJsonFormat
import ir.ipm.bcol.evaluator.PathPropertiesEvaluator
import ir.ipm.bcol.structure.SchemaCreator
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import us.hebi.matlab.mat.format.Mat5.readFromFile

class DataIngestion{

  val logger: Logger = Logger(s"${this.getClass.getName}")

  val MONGO_DB_NAME = "MetaDataDB"
  val MONGO_URI = "mongodb://root:ns123@mongos:27017/admin"
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

      val eventMetaData = schemaCreator.metaDataSchemaCreator(eventEntry, eventMatFile)
      val eventMetaDatJson = spark.createDataset(Seq(eventMetaData.toJson.toString()))
      val eventMetaDatJsonDs = spark.read.json(eventMetaDatJson)
        .withColumn("_id", lit(eventFileName))
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

    FilteredEventDataSet

  }

  def writeChannel(spark: SparkSession, eventDataSet: Dataset[Row], rootDir: String, pathProperties: PathPropertiesEvaluator): Unit={
    import spark.implicits._
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    if(pathProperties.channelFileNames.isEmpty) logger.error("", throw new Exception("Channel file not found"))

    var channelTimeCounter: Long = 0

    val channelFullPath = pathProperties.channelFileNames.map(names => s"$rootDir/$names.mat")

    channelFullPath.foreach(channel => {

      val channelMatFile = readFromFile(channel)
      val channelEntry = channelMatFile.getEntries.asScala
      val channelFileName = fileSystem.getLeafFileName(channel)
      val channelFileHdfsWritePath = s"/${pathProperties.parentPath}/${pathProperties.experimentName}" +
        s"/channels/$channelFileName.parquet"

      logger.info(s"Start Writing Channel MetaData for $channelFileName")

      val channelMetaData = schemaCreator.metaDataSchemaCreator(channelEntry, channelMatFile)
      val channelMetaDatJson = spark.createDataset(Seq(channelMetaData.toJson.toString()))
      val channelMetaDatJsonDs = spark.read.json(channelMetaDatJson)
        .withColumn("_id", lit(channelFileName))
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
    })
  }

}
