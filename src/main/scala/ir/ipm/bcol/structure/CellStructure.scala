package ir.ipm.bcol.structure

import us.hebi.matlab.mat.types.Cell

class CellStructure extends CharStructure with MatrixStructure {

  def parseCellToMap(cellField: Cell, parentName: String): Option[CellType] = {

    val childDimension = cellField.getDimensions.sum
    val charInit = Map(parentName -> "")
    val matInit = Map(parentName -> -1.0)

    if (cellField.getNumElements != 0) {
      if (childDimension <= 2) {

        val cellElemClassNames = cellField.get(0).getClass.toString.split(" ").apply(1)

        val leafValue = cellElemClassNames match {

          case "us.hebi.matlab.mat.format.MatChar" =>
            val charField: CharType = parseRootCharToMap(cellField.getChar(0), parentName)
              .getOrElse(CharType(Left(CharSingle(charInit))))
            CellType(Left(charField))

          case "us.hebi.matlab.mat.format.MatMatrix" =>
            val matrixField: MatrixType = parseMatrixToMap(cellField.getMatrix(0), parentName)
              .getOrElse(MatrixType(Left(MatrixSingle(matInit))))
            CellType(Right(matrixField))

          case _ => throw new Exception(s"Unrecognized type class: $cellElemClassNames")

        }

        Some(leafValue)

      } else {
        None
      }
    } else {
      None
    }

  }

  def parseCellToDataStructure(cellField: Cell, timeIndex: Long, parentName: String): Option[Array[DataStructure]] ={

    val childDimension = cellField.getDimensions.sum

    if (cellField.getNumElements != 0) {
      if (childDimension <= 2) {

        val cellElemClassNames = cellField.get(0).getClass.toString.split(" ").apply(1)

        if (cellElemClassNames.equals("us.hebi.matlab.mat.format.MatMatrix")){

          val result = parseMatrixToDataStructure(cellField.getMatrix(0), timeIndex, parentName)
          if (result.isDefined) Some(result.get) else None

        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }

  }

  def parseCellToEventStructure(cellField: Cell, parentName: String): Option[Array[EventStructure]] ={

    val childDimension = cellField.getDimensions.sum

    if (cellField.getNumElements != 0) {
      if (childDimension <= 2) {

        val cellElemClassNames = cellField.get(0).getClass.toString.split(" ").apply(1)

        if (cellElemClassNames.equals("us.hebi.matlab.mat.format.MatMatrix")){

          val result = parseMatrixToEventStructure(cellField.getMatrix(0), parentName)
          if (result.isDefined) Some(result.get) else None

        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }

  }

}
