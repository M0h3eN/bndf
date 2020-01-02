package ir.ipm.bcol.structure

import us.hebi.matlab.mat.types.{Struct, Matrix, Cell, Char}
import us.hebi.matlab.mat.types.MatFile.Entry
import us.hebi.matlab.mat.format.Mat5File
import com.typesafe.scalalogging.Logger

import scala.collection.JavaConverters._

object MetaDataStructureCreator {

  val logger: Logger = Logger(s"${this.getClass.getName}")

  def parseCharToMap(charField: Char, parentName: String): Option[Map[String, String]] = {

    if (charField.getNumElements != 0) {
      Some(Map(parentName -> charField.toString))
    } else {
      logger.warn(s"$charField is empty, skipping...")
      None
    }
  }

  def parseMatrixToMap(matrixField: Matrix, parentName: String): Option[Map[String, Any]] = {

    val childDimension = matrixField.getDimensions.sum

    if (matrixField.getNumElements != 0) {
      if (childDimension <= 2) {

        val leafValue = matrixField.getDouble(0)
        Some(Map(parentName -> leafValue))

      } else {
        logger.warn(s"Matrix is not flat, skipping $matrixField")
        None
      }
    } else {
      logger.warn(s"$matrixField is empty, skipping...")
      None
    }

  }

  def parseCellToMap(cellField: Cell, parentName: String): Option[Map[String, Any]] = {

    val childDimension = cellField.getDimensions.sum

    if (cellField.getNumElements != 0) {
      if (childDimension <= 2) {

        val cellElemClassNames = cellField.get(0).getClass.toString.split(" ").apply(1)

        val leafValue = cellElemClassNames match {
          case "us.hebi.matlab.mat.format.MatStruct" => parseStructToMap(cellField.getStruct(0), parentName)
          case "us.hebi.matlab.mat.format.MatMatrix" => parseMatrixToMap(cellField.getMatrix(0), parentName).getOrElse(Map(parentName -> ""))
          case "us.hebi.matlab.mat.format.MatChar" => parseCharToMap(cellField.getChar(0), parentName).getOrElse(Map(parentName -> ""))
          case _ => throw new Exception(s"Unrecognized type class: $cellElemClassNames")
        }

        Some(leafValue)

      } else {
        logger.warn(s"Cell is not flat, skipping $cellField")
        None
      }
    } else {
      logger.warn(s"$cellField is empty, skipping...")
      None
    }

  }

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

        mapFields
      }

      structFieldsMapIterator.toArray.map(_.toSeq).reduce(_ ++ _).toMap

    } else {

      val fiealdClassNames = fieldNames.map(cl => structMat.get(cl, 0).getClass.toString.split(" ").apply(1))
      val fiealdInfo = fieldNames.zip(fiealdClassNames).toMap
      val structRowNum = structMat.getDimensions.apply(1)

      val structFieldsMapIterator = fiealdInfo map { case (fn, cl) =>

        val mapFields = cl match {

          case "us.hebi.matlab.mat.format.MatStruct" => Map(fn -> parseStructToMap(structMat.getStruct(fn), fn))
          case "us.hebi.matlab.mat.format.MatCell" => parseCellToMap(structMat.getCell(fn), fn).getOrElse(Map(parentName -> ""))
          case "us.hebi.matlab.mat.format.MatMatrix" => parseMatrixToMap(structMat.getMatrix(fn), fn).getOrElse(Map(parentName -> ""))
          case "us.hebi.matlab.mat.format.MatChar" =>

            val charMapIterator = (1 until structRowNum).map(elem => parseCharToMap(structMat.getChar(fn, elem), fn).getOrElse(Map(parentName -> ""))).toArray
            charMapIterator.map(_.toSeq).reduce(_ ++ _).groupBy(_._1).mapValues(_.map(_._2).toArray.map(st => st.concat(" ")).reduce(_ + _))

          case _ => throw new Exception(s"Unrecognized type class: $cl")

        }

        mapFields

      }

      structFieldsMapIterator.toArray.map(_.toSeq).reduce(_ ++ _).toMap

    }

  }

  def metaDataStructureCreator(entries: Iterable[Entry], matFile: Mat5File): Map[String, Any] = {

    val nestedMetaDataMapIterator = entries.map(it => {

      val parentFieldsName = it.getName
      val parentFieldsType = it.getValue.getType.toString

      val fieldsMap = parentFieldsType match {
        case "char" => parseCharToMap(matFile.getChar(parentFieldsName), parentFieldsName).getOrElse(Map(parentFieldsName -> ""))
        case "struct" => parseStructToMap(matFile.getStruct(parentFieldsName), parentFieldsName)
        case _ => throw new Exception(s"Unrecognized type field: $it")
      }

      Map(parentFieldsName -> fieldsMap)
    })

    val nestedMetaDataMap = nestedMetaDataMapIterator.toArray.map(_.toSeq).reduce(_ ++ _).toMap
    nestedMetaDataMap
  }


}