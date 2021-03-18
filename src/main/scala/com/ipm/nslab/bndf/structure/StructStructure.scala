package com.ipm.nslab.bndf.structure

import us.hebi.matlab.mat.types.Struct

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.immutable.ListMap

/** Provides the decomposition of nested struct type into a standard structure
 * This class extends the CellStructure, MatrixStructure, CharArrayStructure, and CharStructure classes
 */
class StructStructure extends CellStructure {

  /** A recursive call that generates all possible parent and child relation for a given struct into a Map for
   * creating the meta-data
   * @param structMat The corresponding struct field
   * @param parentName The parent name of the struct field
   * @return Nested Map of parents and childes
   */
  def parseStructToMap(structMat: Struct, parentName: String): Map[String, Any] = {

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum


    if (structDimension <= 2) {

      val fieldClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val fieldInfo = fieldNames.zip(fieldClassNames).toMap

      val structFieldsMapIterator: immutable.Iterable[ListMap[String, Any]] = fieldInfo map { case (fn, cl) =>

        val mapFields: Map[String, Any] = cl match {

          case "us.hebi.matlab.mat.format.MatStruct" => Map(fn -> parseStructToMap(structMat.getStruct(fn), fn))
          case "us.hebi.matlab.mat.format.MatCell" => parseCellToMap(structMat.getCell(fn), fn).getOrElse(Map(parentName -> ""))
          case "us.hebi.matlab.mat.format.MatMatrix" => parseMatrixToMap(structMat.getMatrix(fn), fn).getOrElse(Map(parentName -> ""))
          case "us.hebi.matlab.mat.format.MatChar" => parseCharToMap(structMat.getChar(fn), fn).getOrElse(Map(parentName -> ""))
          case _ => throw new Exception(s"Unrecognized type class: $cl")

        }

        val sortedMapField = ListMap(mapFields.toSeq.sortBy(_._1) :_*)
        sortedMapField
      }

      val structMap = structFieldsMapIterator.toArray.map(_.toSeq).reduce(_ ++ _).toMap
      val sortedStructMap = ListMap(structMap.toSeq.sortBy(_._1) :_*)

      sortedStructMap


    } else {

      val fieldClassNames = fieldNames.map(cl => structMat.get(cl, 0).getClass.toString.split(" ").apply(1))
      val fieldInfo = fieldNames.zip(fieldClassNames).toMap
      val structRowNum = structMat.getDimensions.apply(1)

      val structFieldsMapIterator = fieldInfo map { case (fn, cl) =>

        val mapFields = cl match {

          case "us.hebi.matlab.mat.format.MatStruct" => Map(fn -> parseStructToMap(structMat.getStruct(fn), fn))
          case "us.hebi.matlab.mat.format.MatCell" => parseCellToMap(structMat.getCell(fn), fn).getOrElse(Map(parentName -> ""))
          case "us.hebi.matlab.mat.format.MatMatrix" => parseMatrixToMap(structMat.getMatrix(fn), fn).getOrElse(Map(parentName -> ""))
          case "us.hebi.matlab.mat.format.MatChar" => parseCharArrayToMap(structMat, fn, structRowNum)
          case _ => throw new Exception(s"Unrecognized type class: $cl")

        }

        val sortedMapField = ListMap(mapFields.toSeq.sortBy(_._1) :_*)
        sortedMapField

      }

      val structMap = structFieldsMapIterator.toArray.map(_.toSeq).reduce(_ ++ _).toMap
      val sortedStructMap = ListMap(structMap.toSeq.sortBy(_._1) :_*)

      sortedStructMap

    }

  }

  /** A recursive call that extract raw data (Signal values) for a given struct and shape them into the DataStructure type
   * @param structMat The corresponding struct field
   * @param timeIndex Index of the time stamp, ensuring signal-time order
   * @param parentName The parent name of the struct field
   * @return Array of DataStructure which is the desired raw-data structure
   */
  def parseStructToDataStructure(structMat: Struct, timeIndex: Long, parentName: String): Option[Array[DataStructure]] ={

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum


    if (structDimension <= 2) {

      val fieldClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val fieldInfo = fieldNames.zip(fieldClassNames).toMap

      val structFieldsMapIterator = fieldInfo map { case (fn, cl) =>

        if (cl.equals("us.hebi.matlab.mat.format.MatMatrix")){
          val result = parseMatrixToDataStructure(structMat.getMatrix(fn), timeIndex, fn)
          if (result.isDefined) Some(result.get) else None
        } else if (cl.equals("us.hebi.matlab.mat.format.MatCell")){
          val result = parseCellToDataStructure(structMat.getCell(fn), timeIndex, fn)
          if (result.isDefined) Some(result.get) else None
        } else if (cl.equals("us.hebi.matlab.mat.format.MatStruct")){
          val result = parseStructToDataStructure(structMat.getStruct(fn), timeIndex, fn)
          if (result.isDefined) Some(result.get) else None
        } else {
          None
        }
      }

      val structData = structFieldsMapIterator.toArray.flatten
      if(structData.length != 0) Some(structData.flatten) else None

    } else {
      None
    }

  }

  /** A recursive call that extract event information for a given struct and shape them into the EventStructure type
   * Event structure is very similar to data structure
   * @param structMat The corresponding struct field
   * @param parentName The parent name of the struct field
   * @return Array of EventStructure which will be enrich the raw-data structure
   */
  def parseStructToEventStructure(structMat: Struct, parentName: String): Option[Array[EventStructure]] ={

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum


    if (structDimension <= 2) {

      val fieldClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val fieldInfo = fieldNames.zip(fieldClassNames).toMap

      val structFieldsMapIterator = fieldInfo map { case (fn, cl) =>

        if (cl.equals("us.hebi.matlab.mat.format.MatMatrix")){
          val result = parseMatrixToEventStructure(structMat.getMatrix(fn), fn)
          if (result.isDefined) Some(result.get) else None
        } else if (cl.equals("us.hebi.matlab.mat.format.MatCell")){
          val result = parseCellToEventStructure(structMat.getCell(fn), fn)
          if (result.isDefined) Some(result.get) else None
        } else if (cl.equals("us.hebi.matlab.mat.format.MatStruct")){
          val result = parseStructToEventStructure(structMat.getStruct(fn), fn)
          if (result.isDefined) Some(result.get) else None
        } else {
          None
        }
      }

      val structData = structFieldsMapIterator.toArray.flatten
      if(structData.length != 0) Some(structData.flatten) else None

    } else {
      None
    }
  }


  /** A recursive call for search in a struct for a specific field
   * @param structMat The corresponding struct field
   * @param parentName The parent name of the struct field
   * @param field The desired field name
   * @return The value of the desired field name
   */
  def getStructValue(structMat: Struct, parentName: String, field: String): Option[Array[String]] ={

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum

    if (structDimension <= 2) {

      val fieldClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val fieldInfo: Array[(String, String)] = fieldNames.zip(fieldClassNames)

        val structFieldsValue = fieldInfo.map(x => {

          val fn = x._1
          val cl = x._2

          val mapFields = cl match {

            case "us.hebi.matlab.mat.format.MatStruct" => getStructValue(structMat.getStruct(fn), fn, field)
            case "us.hebi.matlab.mat.format.MatCell" => getCellValue(structMat.getCell(fn), fn, field)
            case "us.hebi.matlab.mat.format.MatMatrix" => getMatrixValue(structMat.getMatrix(fn), fn, field)
            case "us.hebi.matlab.mat.format.MatChar" => getCharValue(structMat.getChar(fn), fn, field)
            case _ => None

          }

          mapFields

        })

        val leafValue = structFieldsValue.flatten.flatten

        if (leafValue.isEmpty){
          None
        } else {
          Some(leafValue)
        }


    } else {
      None
    }

  }


}
