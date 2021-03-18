package com.ipm.nslab.bndf.commons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/** Provides All configuration and setting related to Apache Spark
 *
 */
class SparkConfig {

  /** Creates the initial configuration
   * @param appName The application name
   * @param mongoUrl MongoDB URL connection
   * @param mongoDb MongoDB database name
   * @param sqlShufflePar Spark sql shuffle partitions number
   * @return The SparkConf
   */
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
      .set("spark.hadoop.dfs.replication", "3")
      .set("spark.mongodb.output.collection", "")

  }

  /** Creates the spark session based on the provided SparkConf
   * @param conf The configured SparkConf
   * @return The SparkSession
   */
  def sparkSessionCreator(conf: SparkConf) :SparkSession = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    SparkSession
      .builder
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

  }

  /** Provides the Active spark executors list based on an active SparkContext
   * @param sc The active SparkContext
   * @return List of the active executors
   */
  def currentActiveExecutors(sc: SparkContext): Seq[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList
  }

  /** Provides the number of active nodes in the cluster
   * @param sc The active SparkContext
   * @return The number of nodes
   */
  def currentActiveExecutorsCount(sc: SparkContext): Int = {
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).toArray.length
  }

}
