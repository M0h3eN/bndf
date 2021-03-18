package com.ipm.nslab.bndf.structure

import us.hebi.matlab.mat.types.Cell

import scala.collection.immutable.ListMap

/** Provides the decomposition of the Cell type (either Char or Matrix types) into a standard structure
 * This class extends the MatrixStructure, CharArrayStructure, and CharStructure classes
 */
class CellStructure extends CharArrayStructure with MatrixStructure {

  /** Generates all possible parent and child relation for a given cell into a Map for creating the meta-data
   * @param cellField The corresponding cell field
   * @param parentName The parent name of the cell field
   * @return Map of parents and childes
   */
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

        val sortedLeafValue = ListMap(leafValue.toSeq.sortBy(_._1) :_*)

        Some(sortedLeafValue)

      } else {
        None
      }
    } else {
      None
    }

  }

  /** Extract raw data (Signal values) for a given cell and shape them into the DataStructure type
   * @param cellField The corresponding cell field
   * @param timeIndex Index of the time stamp, ensuring signal-time order
   * @param parentName The parent name of the cell field
   * @return Array of DataStructure which is the desired raw-data structure
   */
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

  /** Extract event information for a given cell and shape them into the EventStructure type
   * Event structure is very similar to data structure
   * @param cellField The corresponding cell field
   * @param parentName The parent name of the cell field
   * @return Array of EventStructure which will be enrich the raw-data structure
   */
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

  /** Enables searching in a cell for a specific field
   * @param cellField The corresponding cell field
   * @param parentName The parent name of the cell field
   * @param field The desired field name
   * @return The value of the desired field name
   */
  def getCellValue(cellField: Cell, parentName: String, field: String): Option[Array[String]] ={

    val childDimension = cellField.getDimensions.sum

    if (cellField.getNumElements != 0) {
      if (childDimension <= 2) {

        val cellElemClassNames = cellField.get(0).getClass.toString.split(" ").apply(1)

        val leafValue = cellElemClassNames match {

          case "us.hebi.matlab.mat.format.MatChar" => getCharValue(cellField.getChar(0), parentName, field)
          case _ => None

        }

        leafValue

      } else {
        None
      }
    } else {
      None
    }

  }

}
