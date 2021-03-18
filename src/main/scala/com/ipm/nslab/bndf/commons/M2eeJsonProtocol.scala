package com.ipm.nslab.bndf.commons

import spray.json._

/** Handles configuration related to JSON parsing
 *
 */
object M2eeJsonProtocol extends DefaultJsonProtocol {

  /** Handles parsing of the Map[String, Any] type
   *
   */
  implicit object MapJsonFormat extends JsonFormat[Map[String, Any]] {
    /** A recursive call that converts all Map values to the string type
     * @param m The input Map
     * @return The JsObject of the known type
     */
    def write(m: Map[String, Any]): JsObject = {
      JsObject(m.mapValues {
        case v: String => JsString(v)
        case v: Int => JsNumber(v)
        case v: Map[_, _] => write(v.asInstanceOf[Map[String, Any]])
        case v: Any => JsString(v.toString)
      })
    }

    def read(value: JsValue): Map[String, Any] = ???
  }
}