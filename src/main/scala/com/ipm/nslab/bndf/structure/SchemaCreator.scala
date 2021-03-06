package com.ipm.nslab.bndf.structure

import us.hebi.matlab.mat.format.Mat5File
import us.hebi.matlab.mat.types.MatFile.Entry

import scala.collection.immutable.ListMap


/** Provides schemas for data structures
 *
 */
class SchemaCreator {

  val structStructure = new StructStructure

  /** Evaluates meta-data by constructing a nested JSON schema
   *
   * @param entries All available entries in the ROOT LEVEL of the MAT file
   *                Here only struct or char types are allowed in the ROOT LEVEL
   * @param matFile The input MAT file
   * @return A nested Map type which will be converted to a nested JSON file
   */
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
    val sortedStructMap = ListMap(nestedMetaDataMap.toSeq.sortBy(_._1): _*)

    sortedStructMap
  }

  /** Provides a columnar structure for raw-data
   *
   * @param entries   All available entries in the ROOT LEVEL of the MAT file
   *                  Here only struct or char types are allowed in the ROOT LEVEL
   * @param timeIndex Index of the time stamp, ensuring signal-time order
   * @param matFile   The input MAT file
   * @return Array of DataStructure which will be converted to a Dataset
   */
  def rawDataSchemaCreator(entries: Iterable[Entry], timeIndex: Long, matFile: Mat5File): Array[DataStructure] = {

    val nestedMetaDataMapIterator = entries.map(it => {

      val parentFieldsName = it.getName
      val parentFieldsType = it.getValue.getType.toString

      if (parentFieldsType.equals("struct")) {
        val result = structStructure.parseStructToDataStructure(matFile.getStruct(parentFieldsName), timeIndex, parentFieldsName)
        if (result.isDefined) Some(result.get) else None
      } else {
        None
      }

    })

    val rawData = nestedMetaDataMapIterator.toArray.flatten
    if (rawData.length != 0) rawData.flatten else {
      Array(DataStructure(0, -1))
    }
  }

  /** Provides a columnar structure for event-data
   *
   * @param entries All available entries in the ROOT LEVEL of the MAT file
   *                Here only struct or char types are allowed in the ROOT LEVEL
   * @param matFile The input MAT file
   * @return Array of EventStructure which will be converted to a Dataset
   */
  def eventDataSchemaCreator(entries: Iterable[Entry], matFile: Mat5File): Array[EventStructure] = {

    val nestedMetaDataMapIterator = entries.map(it => {

      val parentFieldsName = it.getName
      val parentFieldsType = it.getValue.getType.toString

      if (parentFieldsType.equals("struct")) {
        val result = structStructure.parseStructToEventStructure(matFile.getStruct(parentFieldsName), parentFieldsName)
        if (result.isDefined) Some(result.get) else None
      } else {
        None
      }

    })

    val rawData = nestedMetaDataMapIterator.toArray.flatten
    if (rawData.length != 0) rawData.flatten else {
      Array(EventStructure(0, "NULL", -1))
    }

  }

  /** Provides searching in the MAT file entries for a specific field
   *
   * @param entries All available entries in the ROOT LEVEL of the MAT file
   *                Here only struct or char types are allowed in the ROOT LEVEL
   * @param matFile The input MAT file
   * @param field   The desired field name
   * @return The value of the desired field name
   */
  def getValue(entries: Iterable[Entry], matFile: Mat5File, field: String): Array[String] = {

    val entryInfo = entries.map(x => (x.getName, x.getValue.getType.toString)).filter(_._1.equalsIgnoreCase(field)).toArray

    if (entryInfo.isEmpty) {

      val nestedMetaDataMapIterator = entries.map(it => {

        val parentFieldsName = it.getName
        val parentFieldsType = it.getValue.getType.toString

        val value: Option[Array[String]] = parentFieldsType match {
          case "struct" => structStructure.getStructValue(matFile.getStruct(parentFieldsName), parentFieldsName, field)
          case _ => None
        }

        value

      })

      val leafValue = nestedMetaDataMapIterator.toArray.flatten.flatten

      if (leafValue.isEmpty) {
        Array("")
      } else {
        leafValue
      }

    } else {

      val parentFieldsType = entryInfo.apply(0)._2
      val parentFieldsName = entryInfo.apply(0)._1

      val value = parentFieldsType match {
        case "char" => structStructure.getCharValue(matFile.getChar(parentFieldsName), parentFieldsName, field)
        case _ => throw new Exception(s"Char type expected but found: $parentFieldsType")
      }

      value.get

    }

  }

}