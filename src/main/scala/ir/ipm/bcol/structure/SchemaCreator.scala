package ir.ipm.bcol.structure

import com.typesafe.scalalogging.Logger
import us.hebi.matlab.mat.types.MatFile.Entry
import us.hebi.matlab.mat.format.Mat5File

import scala.collection.immutable.ListMap


class SchemaCreator {

  val logger: Logger = Logger(s"${this.getClass.getName}")
  val structStructure = new StructStructure

  def metaDataSchemaCreator(entries: Iterable[Entry], matFile: Mat5File): Map[String, Any] = {

    val nestedMetaDataMapIterator = entries.map(it => {

      val parentFieldsName = it.getName
      val parentFieldsType = it.getValue.getType.toString

      val fieldsMap = parentFieldsType match {
        case "char" => structStructure.parseCharToMap(matFile.getChar(parentFieldsName), parentFieldsName).getOrElse(Map(parentFieldsName -> ""))
        case "struct" => structStructure.parseStructToMap(matFile.getStruct(parentFieldsName), parentFieldsName)
        case _ => throw new Exception(s"Unrecognized type field: $it")
      }

      Map(parentFieldsName -> fieldsMap)
    })

    val nestedMetaDataMap = nestedMetaDataMapIterator.toArray.map(_.toSeq).reduce(_ ++ _).toMap
    val sortedStructMap = ListMap(nestedMetaDataMap.toSeq.sortBy(_._1) :_*)

    sortedStructMap
  }

  def dataSchemaCreator(entries: Iterable[Entry], matFile: Mat5File): Array[DataStructure] = {

    val nestedMetaDataMapIterator = entries.map(it => {

      val parentFieldsName = it.getName
      val parentFieldsType = it.getValue.getType.toString

      if(parentFieldsType.equals("struct")){
        val result = structStructure.parseStructToDataStructure(matFile.getStruct(parentFieldsName), parentFieldsName)
        if(result.isDefined) Some(result.get) else None
      } else {
        None
      }

    })

    val rawData = nestedMetaDataMapIterator.toArray.flatten
    if(rawData.length !=0) rawData.apply(0) else {
      logger.warn("File does not have signal data")
      Array(DataStructure(0, -1))
    }
  }

}