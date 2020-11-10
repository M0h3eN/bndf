package com.ipm.nslab.bndf.structure

import us.hebi.matlab.mat.types.Struct

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.immutable.ListMap

class StructStructure extends CellStructure {

  def parseStructToMap(structMat: Struct, parentName: String): Map[String, Any] = {

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum


    if (structDimension <= 2) {

      val fiealdClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val fiealdInfo = fieldNames.zip(fiealdClassNames).toMap

      val structFieldsMapIterator: immutable.Iterable[ListMap[String, Any]] = fiealdInfo map { case (fn, cl) =>

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

      val fiealdClassNames = fieldNames.map(cl => structMat.get(cl, 0).getClass.toString.split(" ").apply(1))
      val fiealdInfo = fieldNames.zip(fiealdClassNames).toMap
      val structRowNum = structMat.getDimensions.apply(1)

      val structFieldsMapIterator = fiealdInfo map { case (fn, cl) =>

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

  def parseStructToDataStructure(structMat: Struct, timeIndex: Long, parentName: String): Option[Array[DataStructure]] ={

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum


    if (structDimension <= 2) {

      val fiealdClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val fiealdInfo = fieldNames.zip(fiealdClassNames).toMap

      val structFieldsMapIterator = fiealdInfo map { case (fn, cl) =>

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

  def parseStructToEventStructure(structMat: Struct, parentName: String): Option[Array[EventStructure]] ={

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum


    if (structDimension <= 2) {

      val fiealdClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val fiealdInfo = fieldNames.zip(fiealdClassNames).toMap

      val structFieldsMapIterator = fiealdInfo map { case (fn, cl) =>

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


  def getStructValue(structMat: Struct, parentName: String, field: String): Option[Array[String]] ={

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum

    if (structDimension <= 2) {

      val fiealdClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val fiealdInfo: Array[(String, String)] = fieldNames.zip(fiealdClassNames)

        val structFieldsValue = fiealdInfo.map(x => {

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
