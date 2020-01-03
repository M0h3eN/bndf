package ir.ipm.bcol

import scala.collection.JavaConverters._
import spray.json._
import ir.ipm.bcol.commons.M2eeJsonProtocol.MapJsonFormat

import us.hebi.matlab.mat.format.Mat5.readFromFile
import ir.ipm.bcol.structure.SchemaCreator
/**
 * @author ${Mohsen.Hadianpour}
 */
object RecordingDataLoader {

  def main(args : Array[String]) {

    val schemaObject = new SchemaCreator

    val matTestFile = readFromFile("/volumes/data/raw-data/TestData/channelik0_1.mat")
    val entries = matTestFile.getEntries.asScala

    val metaData = schemaObject.metaDataSchemaCreator(entries, matTestFile)
    val metaDataJson = metaData.toJson
    println(metaDataJson)

    val rawData = schemaObject.dataSchemaCreator(entries, matTestFile)
    rawData.foreach(println)

  }

}
