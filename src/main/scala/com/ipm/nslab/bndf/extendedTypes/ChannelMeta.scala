package com.ipm.nslab.bndf.extendedTypes

/** Provides channelFile path information which is stored in HDFS
 * @param channelName The name of the channel fiel
 * @param HdfsPath The corresponding path in HDFS
 */
case class ChannelMeta(channelName: String, HdfsPath: String)
