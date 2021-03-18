package com.ipm.nslab.bndf.structure

import us.hebi.matlab.mat.types.Char

/** Provides the decomposition of the char type into a standard structure
 *
 */
class CharStructure {

  /** Generates parent and child relation for a given Char into a Map for creating the meta-data
   * @param charField The corresponding char field
   * @param parentName The char field parent name
   * @return A Map of parent and child
   */
  def parseCharToMap(charField: Char, parentName: String): Option[Map[String, String]] = {

    if (charField.getNumElements != 0) {
      Some(Map(parentName -> charField.toString))
    } else {
      None
    }
  }

  /** Enables searching in a char type for a specific field
   * @param charField The corresponding char field
   * @param parentName The parent name of the char field
   * @param field The desired field name
   * @return The value of the desired field name
   */
  def getCharValue(charField: Char, parentName: String, field: String): Option[Array[String]] = {

    val charValue = Array(charField.toString)

    if(parentName.equalsIgnoreCase(field)){
       Some(charValue)
    } else{
      None
    }
  }



}
