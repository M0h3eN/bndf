package com.ipm.nslab.bdns.commons.io

import com.ipm.nslab.bdns.extendedTypes.ChannelMeta
import org.apache.spark.sql.functions.{input_file_name, element_at, size, split, col}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class SparkReader {

  def channelParquetReader(sparkSession: SparkSession, hdfsChannelDirs: Seq[String]): Dataset[Row] ={

    sparkSession
      .read
      .parquet(hdfsChannelDirs:_*)
      .withColumn("channelName",  split(element_at(split(input_file_name(), "/"), size(split(input_file_name(), "/")) - 1), "\\.").getItem(0))
  }

  def channelParquetReader(sparkSession: SparkSession, hdfsChannelDirs: String): Dataset[Row] ={

    sparkSession
      .read
      .parquet(hdfsChannelDirs + "/*.parquet")
      .withColumn("channelName",  split(element_at(split(input_file_name(), "/"), size(split(input_file_name(), "/")) - 1), "\\.").getItem(0))
  }

  def eventParquetReader(sparkSession: SparkSession, HdfsRootDir: String): Dataset[Row] ={

    sparkSession
      .read
      .parquet(HdfsRootDir + "/*.parquet")
  }

}
