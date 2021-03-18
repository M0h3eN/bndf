package com.ipm.nslab.bndf.extendedTypes

/** Provides The channelFiles counter ordering
 * @param mainCounter The Main counter
 * @param subCounter The sub counter
 *                    The counters are extracted from file naming convention, for example:
 *                    channelik0_1.mat, channelik0_2.mat, channelik0_3.mat, ... --> Main: 0, Sub: 1,2,3, ...
 */
case class ChannelCounters(mainCounter: Int, subCounter: Int)
