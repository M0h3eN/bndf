package com.ipm.nslab.bdns.commons

import com.ipm.nslab.bdns.extendedTypes.ChannelMeta
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.lit

class SparkReader {

  def parquetReader(sparkSession: SparkSession, channelMeta: ChannelMeta): Dataset[Row] ={

    sparkSession
      .read
      .parquet(channelMeta.HdfsPath)
      .withColumn("channelName", lit(channelMeta.channelName))
  }

}
