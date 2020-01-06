package ir.ipm.bcol.commons

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

class MongoConnector {

  def Reader(spark: SparkSession,  uri: String, database: String, collection: String) :DataFrame = {

    val readConfig = ReadConfig(Map("uri" -> uri, "database" -> database, "collection" -> collection))
    MongoSpark.load(spark, readConfig)

  }

  def Writer(spark: SparkSession, DF: DataFrame,  uri: String, database: String, collection: String) :Unit = {

    val writeConfig = WriteConfig(Map("uri" -> uri, "database" -> database, "collection" -> collection))
    MongoSpark.save(DF, writeConfig)
  }

}
