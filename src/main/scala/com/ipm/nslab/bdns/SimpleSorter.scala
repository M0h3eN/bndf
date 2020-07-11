package com.ipm.nslab.bdns

import java.util.concurrent.TimeUnit

import com.ipm.nslab.bdns.commons.io.SparkWriter
import com.ipm.nslab.bdns.commons.{MongoConnector, SparkConfig}
import com.ipm.nslab.bdns.spark.analysis.Sorting
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SaveMode

import scala.util.Try

object SimpleSorter {

  val logger: Logger = Logger(s"${this.getClass.getName}")

  val sparkConfig = new SparkConfig
  val sparkWrite = new SparkWriter
  val HIVE_DB = "sortedDataDB"

  val MONGO_DB_NAME = "MetaDataDB"
  val MONGO_COLLECTION = "Experiments"

  val numberOfSqlPartition = 500

  def main(args: Array[String]): Unit = {

    if(args.isEmpty) logger.error("", throw new Exception("Experiment name must specified"))

    val ex = args.apply(0)
    val MONGO_URI_DEFAULT = "mongodb://root:ns123@mongos:27017/admin"
    val MONGO_URI = Try(args.apply(1)).getOrElse(MONGO_URI_DEFAULT)
    val sorting = new Sorting
    val conf = sparkConfig.sparkInitialConf("Spike Sorter", MONGO_URI, MONGO_DB_NAME, numberOfSqlPartition)
    val spark = sparkConfig.sparkSessionCreator(conf)
    val mongoConnector = MongoConnector(spark, MONGO_URI, MONGO_DB_NAME)

    if (!spark.catalog.databaseExists(HIVE_DB)) spark.sql(s"CREATE DATABASE $HIVE_DB")

    val metaData = mongoConnector.Reader(ex)
    val sortedDataset = sorting.sorter(spark, metaData, ex)

    sparkWrite.writeHiveTable(sortedDataset, SaveMode.Overwrite, HIVE_DB, "SORTED",
      "RecordLocation")

    logger.info("Finished Sorting Spike Trains")
    TimeUnit.SECONDS.sleep(5)
    spark.close()

  }
}