package ir.ipm.bcol.structure

import us.hebi.matlab.mat.types.Struct

trait CharArrayStructure extends CharStructure {

  def parseCharArrayToMap(structField: Struct, parentName: String, rowCount: Int): Map[String, String] ={

    val charMapArray = (1 until rowCount).map(elem => parseCharToMap(structField.getChar(parentName, elem), parentName).getOrElse(Map(parentName -> ""))).toArray
    charMapArray.map(_.toSeq).reduce(_ ++ _).groupBy(_._1).mapValues(_.map(_._2).toArray
      .map(x => x.toString.concat(", ")).foldLeft("[")((x, y) => x + y).foldRight("]")((x, y) => x + y)
      .replace(", ]", "]"))
  }



}
