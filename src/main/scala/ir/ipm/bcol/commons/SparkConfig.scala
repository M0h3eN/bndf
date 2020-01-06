package ir.ipm.bcol.commons

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkConfig {

  def sparkInitialConf(appName: String, mongoUrl: String, mongoDb: String) :SparkConf  = {

    new SparkConf()
      .setAppName(appName)
      .set("spark.network.timeout", "7200")
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .set("spark.mongodb.output.uri", mongoUrl)
      .set("spark.mongodb.output.database", mongoDb)
      .set("spark.mongodb.output.replaceDocument", "true")
      .set("spark.mongodb.output.ordered", "false")
      .set("spark.hadoop.dfs.replication", "3")
      .set("spark.mongodb.output.collection", "")



  }

  def sparkSessionCreator(conf: SparkConf) :SparkSession = {

    SparkSession
      .builder
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

  }


}