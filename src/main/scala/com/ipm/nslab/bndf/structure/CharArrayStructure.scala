package com.ipm.nslab.bndf.structure

import us.hebi.matlab.mat.types.Struct

/** Provides the decomposition of the char array type into a standard structure
 * This trait extends the CharStructure classes
 */
trait CharArrayStructure extends CharStructure {

  /** Generates all possible parent and child relation for a given Char array into a Map for creating the meta-data
   * @param structField The target struct field
   * @param parentName The corresponding parent name
   * @param rowCount Number of elements in the array
   * @return A Map of parent and child (all values in the char array are considered an string)
   */
  def parseCharArrayToMap(structField: Struct, parentName: String, rowCount: Int): Map[String, String] ={

    val charMapArray = (1 until rowCount)
      .map(elem => parseCharToMap(structField.getChar(parentName, elem), parentName).getOrElse(Map(parentName -> "")))
      .toArray

    charMapArray.map(_.toSeq).reduce(_ ++ _).groupBy(_._1).mapValues(_.map(_._2).toArray
      .map(x => x.concat(", "))
      .foldLeft("[")((x, y) => x + y)
      .foldRight("]")((x, y) => x + y)
      .replace(", ]", "]"))
  }



}
