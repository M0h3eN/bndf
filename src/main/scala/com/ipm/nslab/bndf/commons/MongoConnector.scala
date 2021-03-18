package com.ipm.nslab.bndf.commons

import com.ipm.nslab.bndf.extendedTypes.ExperimentMetaDataEvaluator
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/** Provides related configuration for operation in MongoDB from an active spark session
 * @param spark An active spark session
 * @param uri MongoDB URL
 * @param database MongoDB database name
 */
case class MongoConnector(spark: SparkSession,  uri: String, database: String) {


  /** Reads a collection from MongoDB in a DataFrame format
   * @param collection The corresponding collection name
   * @return A dataframe
   */
  def Reader(collection: String): DataFrame = {

    val readConfig = ReadConfig(Map("uri" -> uri, "database" -> database, "collection" -> collection))
    MongoSpark.load(spark, readConfig)

  }

  /** Writes a dataframe in MongoDB collection
   * @param collection The corresponding collection name
   * @param DF The corresponding dataframe
   */
  def Writer(collection: String, DF: DataFrame) :Unit = {

    val writeConfig = WriteConfig(Map("uri" -> uri, "database" -> database, "collection" -> collection))
    MongoSpark.save(DF, writeConfig)
  }


  /** Considers if the experiment has been already processed or not
   * Avoids duplicate processing
   * @param collection The collection name
   * @param column The corresponding column name
   * @param experimentList List of input experiments
   * @return ExperimentMetaDataEvaluator which leads to non-processed experiments
   */
  def checkIfItExist(collection: String, column: String, experimentList: Array[String]): ExperimentMetaDataEvaluator ={

    val metaCollection = Reader(collection)
    val meta: Try[Array[String]] = Try(metaCollection.select(column).collect().map(row => row(0).toString))
    val existedMetaData =  meta.getOrElse(Array("NotExists"))

    val nonExistenceExperiment = existedMetaData.apply(0) match {
      case "NotExists" => experimentList
      case _ => experimentList.diff(existedMetaData)

    }

    ExperimentMetaDataEvaluator(nonExistenceExperiment)

  }
}
