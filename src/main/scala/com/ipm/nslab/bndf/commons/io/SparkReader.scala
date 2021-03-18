package com.ipm.nslab.bndf.commons.io

import org.apache.spark.sql.functions.{input_file_name, element_at, size, split}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** Provides Spark Input operations
 *
 */
class SparkReader {

  /** Reads Channels parquet files from HDFS
   * @param sparkSession An active spark session
   * @param hdfsChannelDirs The sequence of the input paths
   * @return Channels dataset
   */
  def channelParquetReader(sparkSession: SparkSession, hdfsChannelDirs: Seq[String]): Dataset[Row] ={

    sparkSession
      .read
      .parquet(hdfsChannelDirs:_*)
      .withColumn("channelName",  split(element_at(split(input_file_name(), "/"), size(split(input_file_name(), "/")) - 1), "\\.").getItem(0))
  }

  /** Reads Channels parquet files from HDFS
   * @param sparkSession An active spark session
   * @param hdfsChannelDirs The input path
   * @return Channels dataset
   */
  def channelParquetReader(sparkSession: SparkSession, hdfsChannelDirs: String): Dataset[Row] ={

    sparkSession
      .read
      .parquet(hdfsChannelDirs + "/*.parquet")
      .withColumn("channelName",  split(element_at(split(input_file_name(), "/"), size(split(input_file_name(), "/")) - 1), "\\.").getItem(0))
  }

  /** Reads Events parquet files from HDFS
   * @param sparkSession An active spark session
   * @param HdfsRootDir The corresponding root path
   * @return Events dataset
   */
  def eventParquetReader(sparkSession: SparkSession, HdfsRootDir: String): Dataset[Row] ={

    sparkSession
      .read
      .parquet(HdfsRootDir + "/*.parquet")
  }

}
