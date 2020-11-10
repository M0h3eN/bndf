package com.ipm.nslab.bndf.commons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

class SparkConfig {

  def sparkInitialConf(appName: String,  mongoUrl: String, mongoDb: String, sqlShufflePar: Int = 200) :SparkConf  = {

    new SparkConf()
      .setAppName(appName)
      .set("spark.sql.shuffle.partitions", s"$sqlShufflePar")
      .set("spark.sql.broadcastTimeout", "900")
      .set("spark.kryo.registrator", "com.ipm.nslab.bdns.serialization.SparkKryoSerialization")
      .set("spark.kryo.referenceTracking", "true")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.mongodb.output.uri", mongoUrl)
      .set("spark.mongodb.output.database", mongoDb)
      .set("spark.mongodb.output.replaceDocument", "true")
      .set("spark.mongodb.output.ordered", "false")
      .set("spark.hadoop.dfs.replication", "1")
      .set("spark.mongodb.output.collection", "")

  }

  def sparkSessionCreator(conf: SparkConf) :SparkSession = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    SparkSession
      .builder
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

  }

  def currentActiveExecutors(sc: SparkContext): Seq[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList
  }

  def currentActiveExecutorsCount(sc: SparkContext): Int = {
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).toArray.length
  }

}
