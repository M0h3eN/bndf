package com.ipm.nslab.bdns.commons.io

import com.ipm.nslab.bdns.extendedTypes.ChannelMeta
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class SparkReader {

  def parquetReader(sparkSession: SparkSession, channelMeta: ChannelMeta): Dataset[Row] ={

    sparkSession
      .read
      .parquet(channelMeta.HdfsPath)
      .withColumn("channelName", lit(channelMeta.channelName))
  }

}
