package com.ipm.nslab.bndf.extendedTypes

/** Provides the type for the Median measurement
 * @param FileInfo The specific spike trains name (session, experiment, or any identifier that specifies the spike train set)
 * @param median The corresponding median value
 */
case class Median(FileInfo: String, median: Double)
