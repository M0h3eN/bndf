package com.ipm.nslab.bndf.extendedTypes

/** Provides Benchmarking detail structure
 * @param _id A unique id of the process
 * @param moduleName The name of the used module
 * @param experimentName The name of the experiment
 * @param numberOfNodes Total number of nodes used in the cluster
 * @param stage The stage of the module operation
 * @param timeMinute Total elapsed time in minutes
 * @param timeSecond Total elapsed time in seconds
 */
case class BenchmarkDataSet(_id: Int, moduleName: String,
                            experimentName: String, numberOfNodes: Int,
                            stage: String, timeMinute: Double, timeSecond: Double)
