package ir.ipm.bcol.structure

import us.hebi.matlab.mat.types.Matrix

trait MatrixStructure extends CharStructure {

  def parseMatrixToMap(matrixField: Matrix, parentName: String): Option[MatrixType] = {

    val childDimension = matrixField.getDimensions.sum
    val childColumn = matrixField.getNumCols
    val childRow = matrixField.getNumRows

    if (matrixField.getNumElements != 0) {
      if (childDimension <= 2) {

        val leafValue = matrixField.getDouble(0)
        Some(MatrixType(Left(MatrixSingle(Map(parentName -> leafValue)))))

      } else {
        if (childColumn.equals(1)){

          val leafValue = (0 until childRow)
            .map(r => matrixField.getDouble(r))
            .toArray

          Some(MatrixType(Right(MatrixArray(Map(parentName -> leafValue)))))

        } else {
          None
        }
      }
    } else {
      None
    }

  }

  def parseMatrixToDataStructure(matrixField: Matrix, timeIndex: Long,  parentName: String): Option[Array[DataStructure]] = {

    val childDimension = matrixField.getDimensions.sum
    val childColumn = matrixField.getNumCols
    val childRow = matrixField.getNumRows

    if (matrixField.getNumElements != 0){
      if (childColumn.equals(1) && childDimension > 2 && childRow > 100){

        logger.info(s"$parentName --> Signal data found, writing: $matrixField")
        val rawData = (0 until childRow).map(row => DataStructure(matrixField.getDouble(row), row + timeIndex + 1)).toArray
        Some(rawData)

      } else {
        None
      }

    } else {
      None
    }


  }

  def parseMatrixToEventStructure(matrixField: Matrix, parentName: String): Option[Array[EventStructure]] ={

    val childDimension = matrixField.getDimensions.sum
    val childColumn = matrixField.getNumCols
    val childRow = matrixField.getNumRows

    if (matrixField.getNumElements != 0){
    if (childColumn.equals(1) && childDimension > 2 && childRow > 100 && parentName.equals("Data")){

    logger.info(s"$parentName --> Signal data found, writing: $matrixField")

    val rawData = (0 until childRow).map(row => Seq(matrixField.getLong(row), row)).toArray
    val rawDataFlat = (0 until childRow).map(row => matrixField.getLong(row)).toArray

      if (rawDataFlat sameElements rawDataFlat.sorted){
        Some(rawData.map(elem => EventStructure(elem.head, "time", elem.apply(1))))
      } else {
        Some(rawData.map(elem => EventStructure(elem.head, "code", elem.apply(1))))
      }

  } else {
    None
  }

  } else {
    None
  }

  }

}
