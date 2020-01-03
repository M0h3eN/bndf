package ir.ipm.bcol.structure

import us.hebi.matlab.mat.types.Struct

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

class StructStructure extends CellStructure {

  def parseStructToMap(structMat: Struct, parentName: String): Map[String, Any] = {

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum


    if (structDimension <= 2) {

      val fiealdClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val fiealdInfo = fieldNames.zip(fiealdClassNames).toMap

      val structFieldsMapIterator = fiealdInfo map { case (fn, cl) =>

        val mapFields = cl match {

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

}
