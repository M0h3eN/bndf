package ir.ipm.bcol.structure

sealed trait CharType {}

case class CharSingle(charField: Map[String, String]) extends CharType

