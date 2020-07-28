package com.ipm.nslab.bdns.commons

import java.io.File

import com.ipm.nslab.bdns.extendedTypes.{ChannelCounters, PathPropertiesEvaluator, RootPathPropertiesEvaluator}

class FileSystem {

  def getListOfFiles(dir: String, format: String): List[String] = {

    val file = new File(dir)

    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(s".$format"))
      .map(_.toString).toList.sorted

  }

  def getListOfDirs(dir: String): Array[String] ={

    val file = new File(dir)
    file.listFiles().filter(_.isDirectory).map(_.toString)
  }

  def getLeafFileName(rootDir: String): String ={

    val pathLen = rootDir.split("/").length
    rootDir.split("/").apply(pathLen-1).split("\\.").apply(0)
  }

  def getChannelCounterInfo(channel: String): ChannelCounters ={

    val pathLen = channel.split("/").length
    val mainCounter = channel.split("/").apply(pathLen - 1).split("\\.").apply(0).splitAt("channelik".length)._2.split("_").apply(0).toInt
    val subCounter = channel.split("/").apply(pathLen - 1).split("\\.").apply(0).splitAt("channelik".length)._2.split("_").apply(1).toInt

    ChannelCounters(mainCounter, subCounter)
  }

  def getRootPathProperties(rootDir: String): RootPathPropertiesEvaluator ={


    val pathLen = rootDir.split("/").length
    val parentPath = rootDir.split("/").apply(pathLen-2)
    val experimentName = rootDir.split("/").last

    RootPathPropertiesEvaluator(experimentName, parentPath, rootDir)

  }

  def getPathProperties(dir: String, format: String): PathPropertiesEvaluator ={

    val fullPath = getListOfFiles(dir, format)

    val pathLen = fullPath.map(x => x.split("/")).head.length
    val parentPath = fullPath.map(x => x.split("/").apply(pathLen-3)).head
    val experimentName = fullPath.map(x => x.split("/").apply(pathLen-2)).head
    val channelFileNames = fullPath.map(x => x.split("/").apply(pathLen-1).split("\\.").apply(0))
      .filter(c => c.startsWith("channel"))
      .sortBy(x => {
        val mainCounter = x.splitAt("channelik".length)._2.split("_").apply(0).toInt
        val subCounter = x.splitAt("channelik".length)._2.split("_").apply(1).toInt
        (mainCounter, subCounter)
      })
      .toArray
    val eventFileNames = fullPath.map(x => x.split("/").apply(pathLen-1).split("\\.").apply(0))
      .filter(c => c.startsWith("event"))
      .sortBy(x => x.splitAt("eventik".length)._2.split("_").apply(1).toInt)
      .toArray

    PathPropertiesEvaluator(pathLen, parentPath, experimentName, channelFileNames, eventFileNames)

  }



}
