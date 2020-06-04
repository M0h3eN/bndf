package com.ipm.nslab.bdns.structure

import com.typesafe.scalalogging.Logger
import us.hebi.matlab.mat.types.Char

class CharStructure {

  val logger: Logger = Logger(s"${this.getClass.getName}")

  def parseCharToMap(charField: Char, parentName: String): Option[Map[String, String]] = {

    if (charField.getNumElements != 0) {
      Some(Map(parentName -> charField.toString))
    } else {
      None
    }
  }

  def getCharValue(charField: Char, parentName: String, field: String): Option[Array[String]] = {

    val charValue = Array(charField.toString)

    if(parentName.equalsIgnoreCase(field)){
       Some(charValue)
    } else{
      None
    }
  }



}
