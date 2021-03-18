package com.ipm.nslab.bndf.extendedTypes

/** Provides the detailed information about the path and naming of the raw files
 * @param pathLen  The '/' splitted path length
 * @param parentPath The parent path which is specified by the preceding '/'
 * @param experimentName The name of the experiment
 * @param channelFileNames All available channelFile names in the specified path
 * @param eventFileNames All available eventFile names in the specified path
 */
case class PathPropertiesEvaluator(pathLen: Int, parentPath: String,
                                   experimentName: String, channelFileNames: Array[String],
                                   eventFileNames: Array[String])
