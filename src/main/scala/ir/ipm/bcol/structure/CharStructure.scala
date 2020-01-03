package ir.ipm.bcol.structure

import com.typesafe.scalalogging.Logger
import us.hebi.matlab.mat.types.Char

class CharStructure {

  val logger: Logger = Logger(s"${this.getClass.getName}")

  def parseCharToMap(charField: Char, parentName: String): Option[Map[String, String]] = {

    if (charField.getNumElements != 0) {
      Some(Map(parentName -> charField.toString))
    } else {
      logger.warn(s"$parentName --> $charField is empty, skipping...")
      None
    }
  }

}
