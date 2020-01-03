package ir.ipm.bcol.structure

import us.hebi.matlab.mat.types.Cell

import scala.collection.immutable.ListMap

class CellStructure extends CharArrayStructure with MatrixStructure {

  def parseCellToMap(cellField: Cell, parentName: String): Option[Map[String, Any]] = {

    val childDimension = cellField.getDimensions.sum

    if (cellField.getNumElements != 0) {
      if (childDimension <= 2) {

        val cellElemClassNames = cellField.get(0).getClass.toString.split(" ").apply(1)

        val leafValue = cellElemClassNames match {

          case "us.hebi.matlab.mat.format.MatMatrix" => parseMatrixToMap(cellField.getMatrix(0), parentName).getOrElse(Map(parentName -> ""))
          case "us.hebi.matlab.mat.format.MatChar" => parseCharToMap(cellField.getChar(0), parentName).getOrElse(Map(parentName -> ""))
          case _ => throw new Exception(s"Unrecognized type class: $cellElemClassNames")

        }

        val sorteLeafValue = ListMap(leafValue.toSeq.sortBy(_._1) :_*)

        Some(sorteLeafValue)

      } else {
        logger.warn(s"$parentName --> cell is not flat, skipping $cellField")
        None
      }
    } else {
      logger.warn(s"$parentName --> $cellField is empty, skipping...")
      None
    }

  }

}
