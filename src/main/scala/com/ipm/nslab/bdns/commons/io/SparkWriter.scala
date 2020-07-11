package com.ipm.nslab.bdns.commons.io

import org.apache.spark.sql.{Dataset, Row, SaveMode}

class SparkWriter {

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
}
