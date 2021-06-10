package com.winwire.adobe.jdbc

import java.sql.Timestamp

private[jdbc] object JdbcUtils {

  def concatenateStrings(seq: Iterable[String]): String = seq.mkString(", ")

  def toSqlTypes(seq: Iterable[Any]): Iterable[String] = {
    seq.map(toSqlType)
  }

  def toSqlType(any: Any): String = {
    any match {
      case string: String => s"'$string'"
      case timestamp: Timestamp => s"'$timestamp'"
      case boolean: Boolean => if (boolean) "1" else "0"
      case byte: Byte => byte.toString
      case int: Int => int.toString
      case long: Long => long.toString
      case float: Float => float.toString
      case double: Double => double.toString
      case scalaType => throw new ClassNotFoundException(s"$scalaType is not supported")
    }
  }

  def getCaseClassParams[A](caseClass: A, fieldToExclude: Option[String] = None): Map[String, Any] = {
    val result = caseClass.getClass.getDeclaredFields
      .filterNot(_.getName.toLowerCase.startsWith("$"))
      .foldLeft(Map.empty[String, Any]) { (map, field) =>
        field.setAccessible(true)
        map + (field.getName -> field.get(caseClass))
      }
    if (fieldToExclude.isDefined) result - fieldToExclude.get.toLowerCase() else result
  }
}
