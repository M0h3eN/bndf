package ir.ipm.bcol.commons

import us.hebi.matlab.mat.format.{MatStruct, MatMatrix}
import us.hebi.matlab.mat.types.{Struct, Matrix, Cell, Char}

import scala.collection.JavaConverters._

object MapCreator {

  def mapCreator(structMat: Struct) = {

    val FieldNames = structMat.getFieldNames.asScala.toArray

    if (FieldNames.length > 1){

      val FiealdClassNames = FieldNames.map(cl => structMat.get(cl).getClass.toString.split(" ").apply(1))
      val FiealdInfo = FieldNames.zip(FiealdClassNames).toMap

      FiealdInfo map { case (fn, cl) => {

        val mapFields = cl match {

          case "us.hebi.matlab.mat.format.MatMatrix" => {
            val child = structMat.getMatrix(fn)
            if (child.getNumElements != 0) {
              val elemType = child.getType.toString
              val leafValue = elemType match {
                case "double" => child.getDouble(0)
              }
            }
          }

        }
      }}

    } else {



    }


  }

}
