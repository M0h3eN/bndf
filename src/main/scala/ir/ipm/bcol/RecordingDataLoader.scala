package ir.ipm.bcol

import scala.collection.JavaConverters._
import spray.json._
import ir.ipm.bcol.commons.M2eeJsonProtocol.MapJsonFormat

import us.hebi.matlab.mat.format.Mat5.readFromFile
import ir.ipm.bcol.structure.MetaDataStructureCreator.metaDataStructureCreator
/**
 * @author ${user.name}
 */
object RecordingDataLoader {

  def main(args : Array[String]) {

    val matTestFile = readFromFile("/volumes/data/raw-data/TestData/channelik0_1.mat")
    val entries = matTestFile.getEntries.asScala

    val metaData = metaDataStructureCreator(entries, matTestFile)

    val metaDataJson = metaData.toJson
    println(metaDataJson)

  }

}
