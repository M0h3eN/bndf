package com.ipm.nslab.bdns.extendedTypes

case class BenchmarkDataSet(_id: Int, moduleName: String,
                            experimentName: String, numberOfNodes: Int,
                            stage: String, timeMinute: Double, timeSecond: Double)
