package com.ipm.nslab.bdns.commons

import com.ipm.nslab.bdns.evaluator.ExperimentMetaDataEvaluator
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

case class MongoConnector(spark: SparkSession,  uri: String, database: String) {


  def Reader(collection: String): DataFrame = {

    val readConfig = ReadConfig(Map("uri" -> uri, "database" -> database, "collection" -> collection))
    MongoSpark.load(spark, readConfig)

  }

  def Writer(collection: String, DF: DataFrame) :Unit = {

    val writeConfig = WriteConfig(Map("uri" -> uri, "database" -> database, "collection" -> collection))
    MongoSpark.save(DF, writeConfig)
  }


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
