package ir.ipm.bcol.commons


import spray.json._

object M2eeJsonProtocol extends DefaultJsonProtocol {

  implicit object MapJsonFormat extends JsonFormat[Map[String, Any]] { // 1
    def write(m: Map[String, Any]): JsObject = {
      JsObject(m.mapValues {                  // 2
        case v: String => JsString(v)         // 3
        case v: Int => JsNumber(v)
        case v: Map[_, _] => write(v.asInstanceOf[Map[String, Any]])  // 4
        case v: Any => JsString(v.toString)   // 5
      })
    }

    def read(value: JsValue) = ???            // 6
  }
}