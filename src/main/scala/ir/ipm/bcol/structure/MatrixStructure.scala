package ir.ipm.bcol.structure

import us.hebi.matlab.mat.types.Matrix

trait MatrixStructure extends CharStructure {

  def parseMatrixToMap[M](matrixField: Matrix, parentName: String): Option[Map[String, M]] = {

    val childDimension = matrixField.getDimensions.sum
    val childColumn = matrixField.getNumCols
    val childRow = matrixField.getNumRows

    if (matrixField.getNumElements != 0) {
      if (childDimension <= 2) {

        val leafValue = matrixField.getDouble(0).asInstanceOf[M]
        Some(Map(parentName -> leafValue))

      } else {
        if (childColumn.equals(1) && childRow < 100){

          val leafValue = (0 until childRow).map(r => matrixField.getDouble(r)).toArray
            .map(x => x.toString.concat(", ")).foldLeft("[")((x, y) => x + y).foldRight("]")((x, y) => x + y)
            .replace(", ]", "]")

          Some(Map(parentName -> leafValue.asInstanceOf[M]))

        } else {
          logger.warn(s"$parentName --> to much for meta data, skipping $matrixField")
          None
        }
      }
    } else {
      logger.warn(s"$parentName --> $matrixField is empty, skipping...")
      None
    }

  }

}
