package ir.ipm.bcol.structure

sealed trait MatTypeHierarchy[+C <: MatTypeHierarchy[_]]

case class CharSingle(charField: Map[String, String]) extends MatTypeHierarchy[Map[String, String]]
case class CharArray(charArrayField: Map[String, Array[String]]) extends MatTypeHierarchy[Map[String, Array[String]]]

case class MatrixSingle(matrixField: Map[String, Double]) extends MatTypeHierarchy[Map[String, Double]]
case class MatrixArray(matrixArrayField: Map[String, Array[Double]]) extends MatTypeHierarchy[Map[String, Array[Double]]]

case class CharType(field: Either[CharSingle, CharArray]) extends MatTypeHierarchy[Either[CharSingle, CharArray]]
case class MatrixType(field: Either[MatrixSingle, MatrixArray]) extends MatTypeHierarchy[Either[MatrixSingle, MatrixArray]]
case class CellType(field: Either[CharType, MatrixType]) extends MatTypeHierarchy[Either[CharType, MatrixType]]

//case class StructType[C](field: MatTypeHierarchy[C]) extends MatTypeHierarchy[C]
case class StructType[A <: MatTypeHierarchy[_]](field: Map[String, StructType[A]]) extends MatTypeHierarchy[Map[String, A]]
case class StructNestedType[A <: MatTypeHierarchy[_]](field: Map[String, A]) extends MatTypeHierarchy[Map[String, A]]






