package com.ipm.nslab.bndf.commons.io

import org.apache.spark.sql.{Dataset, Row, SaveMode}

/** Provides Spark Output operations
 *
 */
class SparkWriter {

  /** Writes a dataset as a Hive table
   * @param dataset The input dataset
   * @param saveMode The Spark saveMode
   * @param dataBase The Hive database
   * @param tableName The Hive table name
   * @param partitionColumn The partition column(s) if needed
   */
  def writeHiveTable(dataset: Dataset[Row], saveMode: SaveMode,
                     dataBase: String, tableName: String, partitionColumn: String*): Unit={

    if(partitionColumn.equals(null)){

      dataset
        .write
        .mode(saveMode)
        .saveAsTable(dataBase + "." + tableName)
    } else {

      dataset
        .write
        .mode(saveMode)
        .partitionBy(partitionColumn: _*)
        .saveAsTable(dataBase + "." + tableName)
    }

  }

  /** Writes a dataset as Parquet file in HDFS
   * @param dataset The input dataset
   * @param saveMode The Spark saveMode
   * @param writePath The destination path in HDFS
   * @param partitionColumn The partition column(s) if needed
   */
  def writeParquetInHdfs(dataset: Dataset[Row], saveMode: SaveMode, writePath: String,
                         partitionColumn: String*): Unit ={


    if(partitionColumn.equals(null)){

      dataset.write
        .mode(saveMode)
        .parquet(writePath)

    } else {

      dataset.write
        .mode(saveMode)
        .partitionBy(partitionColumn: _*)
        .parquet(writePath)
    }

  }
}
