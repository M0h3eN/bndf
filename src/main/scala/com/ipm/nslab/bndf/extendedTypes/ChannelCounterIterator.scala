package com.ipm.nslab.bndf.extendedTypes

/** Provides the channel counter iterator naming information
 * @param fullPath The full path of the file
 * @param channelFileName The name of the file
 * @param channelFileInfo The information about the recording extracted from meta-data
 * @param mainCounter The Main counter
 * @param subCounter The sub counter
 *                    The counters are extracted from file naming convention, for example:
 *                    channelik0_1.mat, channelik0_2.mat, channelik0_3.mat, ... --> Main: 0, Sub: 1,2,3, ...
 */
case class ChannelCounterIterator(fullPath: String, channelFileName: String, channelFileInfo: String,
                                  mainCounter: Int, subCounter: Int)
