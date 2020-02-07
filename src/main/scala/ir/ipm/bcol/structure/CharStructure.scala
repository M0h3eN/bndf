package ir.ipm.bcol.structure

import com.typesafe.scalalogging.Logger
import us.hebi.matlab.mat.types.{Char, Struct}

class CharStructure {

  val logger: Logger = Logger(s"${this.getClass.getName}")

  def parseCharToMap(structField: Struct, charFieldName: String, parentName: String): Option[CharType] = {

    val structRowNumber = structField.getDimensions.apply(1)
    val structDimension = structField.getDimensions.sum

    if (structDimension <= 2) {
      val charField: Char = structField.getChar(charFieldName)

      if (charField.getNumElements != 0) {
        Some(CharType(Left(CharSingle(Map(parentName -> charField.toString)))))
      } else {
        None
      }
    } else {

      val charMapArray = (1 until structRowNumber)
        .map(elem => CharSingle(Map(parentName -> structField.getChar(parentName, elem).toString)))
        .toArray
        .map(_.charField).reduce(_ ++ _).groupBy(_._1).mapValues(_.map(_._2).toArray)

      Some(CharType(Right(CharArray(charMapArray))))

    }
  }

  def parseRootCharToMap(charField: Char, parentName: String): Option[CharType] = {

    if (charField.getNumElements != 0) {
      Some(CharType(Left(CharSingle(Map(parentName -> charField.toString)))))
    } else {
      None
    }
  }

}
