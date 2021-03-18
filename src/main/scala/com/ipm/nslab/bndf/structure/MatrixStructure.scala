package com.ipm.nslab.bndf.structure

import us.hebi.matlab.mat.types.Matrix

/** Provides the decomposition of the Matrix type into a standard structure
 * This trait extends the CharArrayStructure, and CharStructure classes
 */
trait MatrixStructure extends CharStructure {

  /** Generates all possible parent and child relation for a given matrix into a Map for creating the meta-data
   * @param matrixField The corresponding matrix field
   * @param parentName The parent name of the matrix field
   * @tparam M Polymorphic type M for handling different input type
   *           Here all types will be mapped to String since we are dealing with meta-data
   * @return A Map of parent and child
   */
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
            .map(x => x.toString.concat(", "))
            .foldLeft("[")((x, y) => x + y)
            .foldRight("]")((x, y) => x + y)
            .replace(", ]", "]")

          Some(Map(parentName -> leafValue.asInstanceOf[M]))

        } else {
          None
        }
      }
    } else {
      None
    }

  }

  /** Extract raw data (Signal values) for a given matrix and shape them into the DataStructure type
   * @param matrixField The corresponding matrix field
   * @param timeIndex Index of the time stamp, ensuring signal-time order
   * @param parentName The parent name of the matrix field
   * @return Array of DataStructure which is the desired raw-data structure
   */
  def parseMatrixToDataStructure(matrixField: Matrix, timeIndex: Long,  parentName: String): Option[Array[DataStructure]] ={

    val childDimension = matrixField.getDimensions.sum
    val childColumn = matrixField.getNumCols
    val childRow = matrixField.getNumRows

    if (matrixField.getNumElements != 0){
      if (childColumn.equals(1) && childDimension > 2 && childRow > 100){

        val rawData = (0 until childRow).map(row => DataStructure(matrixField.getDouble(row), row + timeIndex + 1)).toArray
        Some(rawData)

      } else {
        None
      }

    } else {
      None
    }


  }

  /** Extract event information for a given matrix and shape them into the EventStructure type
   * Event structure is very similar to data structure
   * @param matrixField The corresponding matrix field
   * @param parentName The parent name of the cell field
   * @return Array of EventStructure which will be enrich the raw-data structure
   */
  def parseMatrixToEventStructure(matrixField: Matrix, parentName: String): Option[Array[EventStructure]] ={

    val childDimension = matrixField.getDimensions.sum
    val childColumn = matrixField.getNumCols
    val childRow = matrixField.getNumRows

    if (matrixField.getNumElements != 0){
    if (childColumn.equals(1) && childDimension > 2 && childRow > 100 && parentName.equals("Data")){

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

  /** Enables searching in a matrix for a specific field
   * @param matrixField The corresponding matrix field
   * @param parentName The parent name of the matrix field
   * @param field The desired field name
   * @return The value of the desired field name
   */
  def getMatrixValue(matrixField: Matrix, parentName: String, field: String): Option[Array[String]] = {

    val childDimension = matrixField.getDimensions.sum
    val childColumn = matrixField.getNumCols
    val childRow = matrixField.getNumRows

    if(parentName.equalsIgnoreCase(field)) {

      if (matrixField.getNumElements != 0) {
        if (childDimension <= 2) {

          val leafValue = matrixField.getDouble(0).toString
          Some(Array(leafValue))

        } else {
          if (childColumn.equals(1) && childRow < 100) {

            val leafValue = (0 until childRow).map(r => matrixField.getDouble(r).toString).toArray

            Some(leafValue)

          } else {
            None
          }
        }
      } else {
        None
      }
    } else {
      None
    }

  }


}
