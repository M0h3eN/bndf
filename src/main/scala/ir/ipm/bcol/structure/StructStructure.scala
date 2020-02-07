package ir.ipm.bcol.structure

import us.hebi.matlab.mat.types.Struct

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

class StructStructure extends CellStructure {

  def parseStructToMap[A >: StructType[A]](structMat: Struct, parentName: String): Map[String, A] = {

    val fieldNames = structMat.getFieldNames.asScala.toArray
    val structDimension = structMat.getDimensions.sum

    val charInit = Map(parentName -> "")
    val matInit = Map(parentName -> -1.0)

   val fiealdInfo: Map[String, String] =  if (structDimension <= 2) {

       val fiealdClassNames = fieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
        fieldNames.zip(fiealdClassNames).toMap

    } else {

       val fiealdClassNames = fieldNames.map(cl => structMat.get(cl, 0).getClass.toString.split(" ").apply(1))
         fieldNames.zip(fiealdClassNames).toMap
    }

      val structFieldsMapIterator = fiealdInfo map { case (fn, cl) =>

        val mapFields: ListMap[String, A] = cl match {

          case "us.hebi.matlab.mat.format.MatStruct" =>
            val struct: StructNestedType[Map[String, A]] = StructNestedType(Map(fn -> parseStructToMap[A](structMat.getStruct(fn), fn)))
            val returnValue: Map[String, Map[String, Any]] = ListMap(struct.field.toSeq.sortBy(_._1): _*)
            returnValue


          case "us.hebi.matlab.mat.format.MatCell" =>
            val cell: CellType = parseCellToMap(structMat.getCell(fn), fn)
              .getOrElse(CellType(Left(CharType(Left(CharSingle(charInit))))))

            val cellMatch = cell.field match {

              case Left(a) => a.field match {
                case Left(a0) => StructNestedType(CharSingle(a0.charField).charField)
                case Right(a1) => StructNestedType(CharArray(a1.charArrayField).charArrayField)
              }

              case Right(b) => b.field match {
                case Left(b0) => StructNestedType(MatrixSingle(b0.matrixField).matrixField)
                case Right(b1) => StructNestedType(MatrixArray(b1.matrixArrayField).matrixArrayField)
              }

            }

            val returnValue = ListMap(cellMatch.field.toSeq.sortBy(_._1): _*)
            returnValue

          case "us.hebi.matlab.mat.format.MatMatrix" =>
            val matrix = parseMatrixToMap(structMat.getMatrix(fn), fn)
              .getOrElse(MatrixType(Left(MatrixSingle(matInit))))

            val matrixMatch = matrix.field match {
              case Left(a) => StructNestedType(MatrixSingle(a.matrixField).matrixField)
              case Right(b) => StructNestedType(MatrixArray(b.matrixArrayField).matrixArrayField)
            }

            val returnValue = ListMap(matrixMatch.field.toSeq.sortBy(_._1): _*)
            returnValue

          case "us.hebi.matlab.mat.format.MatChar" =>
            val char: CharType = parseCharToMap(structMat, fn, fn)
              .getOrElse(CharType(Left(CharSingle(charInit))))

            val charMatch = char.field match {
              case Left(a) => StructNestedType(CharSingle(a.charField).charField)
              case Right(b) => StructNestedType(CharArray(b.charArrayField).charArrayField)
            }

            val returnValue = ListMap(charMatch.field.toSeq.sortBy(_._1): _*)
            returnValue

          case _ => throw new Exception(s"Unrecognized type class: $cl")

        }

        val sortedMapField = ListMap(mapFields.toSeq.sortBy(_._1): _*)
        sortedMapField

      }

      val structMap = structFieldsMapIterator.toArray.map(_.toSeq).reduce(_ ++ _).toMap
      val sortedStructMap = ListMap(structMap.toSeq.sortBy(_._1): _*).toMap

      StructNestedType(sortedStructMap)

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

}
