package com.ipm.nslab.bndf.extendedTypes

/** Provides information about paths needed for meta-dat
 * @param _id The experiment name
 * @param parentPath The parent path which is specified by the preceding '/'
 * @param fullPath The full path
 */
case class RootPathPropertiesEvaluator(_id: String, parentPath: String, fullPath: String)
