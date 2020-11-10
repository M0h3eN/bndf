package com.ipm.nslab.bndf.commons

import java.io.File
import java.nio.file.Files.copy
import java.nio.file.Paths.get

import scala.util.Try

object RootDirOperation {

  def main(args: Array[String]): Unit = {

    val fileSystem = new FileSystem

    val rootPath = "/run/media/mohsen/Elements/Main-mat"
    val folderName = "Experiment-10G"

    implicit def toPath (filename: String) = get(filename)

    val dirList = fileSystem.getListOfDirs(rootPath)//.filter(_.startsWith("2017"))
    val subDirs = dirList.flatMap(x => fileSystem.getListOfFiles(x, "mat"))
    val pathLen = subDirs.map(x => x.split("/")).head.length

    val channelFileNames: Array[(String, String, Int, Int)] = subDirs
      .filter(x => x.split("/").apply(pathLen - 1).split("\\.").apply(0).startsWith("channel"))
      .map(x => {

        val originalPath = x
        val experimentName = x.split("/").apply(pathLen - 2).split("\\.").apply(0)
        val mainCounter = x.split("/").apply(pathLen - 1).split("\\.").apply(0).splitAt("channelik".length)._2.split("_").apply(0).toInt
        val subCounter = x.split("/").apply(pathLen - 1).split("\\.").apply(0).splitAt("channelik".length)._2.split("_").apply(1).toInt
        (originalPath, experimentName, mainCounter, subCounter)
      })
      .filter(_._2.startsWith("2017"))
//      .filter(_._2.split("-").apply(2).split("_").apply(0).toInt <= 8)
      .filter(x => (x._2.equals("2017-09-03_07-52") || x._2.equals("2017-09-05_10-06")))
      .sortBy(x => (x._2, x._3, x._4))
      .toArray

//    val testDir = channelFileNames.map(_._2).distinct.sorted
    var dummyCounter = 0

    val changedFileNames = channelFileNames.indices.map(x => {
      val changedIndices = if(channelFileNames.apply(x)._3 == Try(channelFileNames.apply(x -1)._3).getOrElse(0)){
        dummyCounter
      } else if (channelFileNames.apply(x)._3 - Try(channelFileNames.apply(x -1)._3).getOrElse(0) == 1){
        dummyCounter = dummyCounter + 1
        dummyCounter
      } else {
        dummyCounter = dummyCounter + 1
        dummyCounter
      }
      (channelFileNames.apply(x)._1, rootPath + "/" + folderName +
        "/" + "channelik" + changedIndices + "_" + channelFileNames.apply(x)._4 + ".mat")
    })

    val eventFileNames = subDirs
      .filter(x => x.split("/").apply(pathLen-1).split("\\.").apply(0).startsWith("eventik"))
      .map(x => {

        val originalPath = x
        val experimentName = x.split("/").apply(pathLen-2).split("\\.").apply(0)
        val mainCounter = x.split("/").apply(pathLen-1).split("\\.").apply(0).splitAt("eventik".length)._2.split("_").apply(1).toInt
        (originalPath, experimentName, mainCounter)
      })
      .filter(_._2.startsWith("2017"))
//      .filter(_._2.split("-").apply(2).split("_").apply(0).toInt <= 8)
      .filter(x => (x._2.equals("2017-09-03_07-52") || x._2.equals("2017-09-05_10-06")))
      .sortBy(x => (x._2, x._3))
      .toArray

    var dummyCounterEvent = 1

    val eventChangedFileNames = eventFileNames.indices.map(x => {
      val changedIndices = if(eventFileNames.apply(x)._3 == Try(eventFileNames.apply(x -1)._3).getOrElse(1)){
        dummyCounterEvent
      } else if (eventFileNames.apply(x)._3 - Try(eventFileNames.apply(x -1)._3).getOrElse(1) == 1){
        dummyCounterEvent = dummyCounterEvent + 1
        dummyCounterEvent
      } else {
        dummyCounterEvent = dummyCounterEvent + 1
        dummyCounterEvent
      }
      (eventFileNames.apply(x)._1, rootPath + "/" + folderName +
        "/" + "eventik" + "_" + changedIndices + ".mat")
    })

    val combinedFiles = changedFileNames ++ eventChangedFileNames

    new File(rootPath + "/" + folderName).mkdir()
    combinedFiles.foreach(file => copy(file._1, file._2))

  }
}
