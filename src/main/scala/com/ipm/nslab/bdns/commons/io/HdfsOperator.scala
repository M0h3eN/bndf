package com.ipm.nslab.bdns.commons.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

class HdfsOperator {

  def deleteFileBasedOnDir(basePath: String, config: Option[Configuration] = None): Unit = {

    val conf = config.getOrElse(new Configuration)
    val hdfsFileSystem = FileSystem.get(conf)
    val hdfsPath = new Path(basePath)

    hdfsFileSystem.delete(hdfsPath, true)

  }

}
