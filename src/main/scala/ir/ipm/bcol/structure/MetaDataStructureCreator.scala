package ir.ipm.bcol.structure

import us.hebi.matlab.mat.types.MatFile.Entry
import us.hebi.matlab.mat.format.Mat5File
import ir.ipm.bcol.structure.StructStructure

import scala.collection.immutable.ListMap


object MetaDataStructureCreator {

  def metaDataStructureCreator(entries: Iterable[Entry], matFile: Mat5File): Map[String, Any] = {

    val structObj = new StructStructure

    val nestedMetaDataMapIterator = entries.map(it => {

      val parentFieldsName = it.getName
      val parentFieldsType = it.getValue.getType.toString

      val fieldsMap = parentFieldsType match {
        case "char" => structObj.parseCharToMap(matFile.getChar(parentFieldsName), parentFieldsName).getOrElse(Map(parentFieldsName -> ""))
        case "struct" => structObj.parseStructToMap(matFile.getStruct(parentFieldsName), parentFieldsName)
        case _ => throw new Exception(s"Unrecognized type field: $it")
      }

      Map(parentFieldsName -> fieldsMap)
    })

    val nestedMetaDataMap = nestedMetaDataMapIterator.toArray.map(_.toSeq).reduce(_ ++ _).toMap
    val sortedStructMap = ListMap(nestedMetaDataMap.toSeq.sortBy(_._1) :_*)

    sortedStructMap
  }


}