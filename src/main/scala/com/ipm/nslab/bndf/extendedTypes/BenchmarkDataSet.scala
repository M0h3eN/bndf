package com.ipm.nslab.bndf.extendedTypes

case class BenchmarkDataSet(_id: Int, moduleName: String,
                            experimentName: String, numberOfNodes: Int,
                            stage: String, timeMinute: Double, timeSecond: Double)
