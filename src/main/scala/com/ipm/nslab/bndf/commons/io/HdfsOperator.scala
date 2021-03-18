package com.ipm.nslab.bndf.commons.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

/** Provides operations related to HDFS
 *
 */
class HdfsOperator {

  /** Enables deleting files on HDFS
   * @param basePath Base pathy in HDFS
   * @param config Extra HDFS configuration if required
   */
  def deleteFileBasedOnDir(basePath: String, config: Option[Configuration] = None): Unit = {

    val conf = config.getOrElse(new Configuration)
    val hdfsFileSystem = FileSystem.get(conf)
    val hdfsPath = new Path(basePath)

    hdfsFileSystem.delete(hdfsPath, true)

  }

}
