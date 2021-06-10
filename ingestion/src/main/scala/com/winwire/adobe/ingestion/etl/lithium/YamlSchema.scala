package com.winwire.adobe.ingestion.etl.lithium

import org.yaml.snakeyaml.Yaml

import java.io.FileReader
import java.util
import java.util.Objects.requireNonNull

case class Column(number: Int, name: String, dataType: String)

case class LiteralColumn(name: String, value: AnyVal)

class YamlSchema(val tableName: String,
                 val columns: List[Column],
                 val partitionColums: Option[List[Column]] = None,
                 val literalColumns: Option[List[LiteralColumn]] = None) {
  override def equals(that: scala.Any): Boolean = {
    that match {
      case thatObj: YamlSchema => thatObj.canEqual(this) && this.tableName == thatObj.tableName && this.columns == thatObj.columns
      case _ => false
    }
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[YamlSchema]

  def columnsList(): List[String] = {
    val columns = for (column <- this.columns) yield column.name
    columns
  }

  def hiveColumnsList(): List[String] = {
    val columnNames = for (column <- this.columns;
                           columnName = column.name.replace(".", "_") if !partitionColums.get.contains(column)
                           ) yield columnName
    columnNames
  }
}

object YamlSchema {
  def fromYamlText(schemaText: String): YamlSchema = {
    fromYaml((new Yaml).load(schemaText))
  }

  private def fromYaml(yaml: AnyRef): YamlSchema = {
    def getArrayAttribute(document: util.LinkedHashMap[String, AnyVal], attribute: String) =
      Option(document.get(attribute).asInstanceOf[util.ArrayList[util.LinkedHashMap[String, AnyVal]]])

    def getStringAttribute(document: util.LinkedHashMap[String, AnyVal], attribute: String) =
      Option(document.get(attribute).asInstanceOf[String])

    val document: util.LinkedHashMap[String, AnyVal] = yaml.asInstanceOf[util.LinkedHashMap[String, AnyVal]]

    val table = getStringAttribute(document, "table") match {
      case Some(x) => x
      case None => throw new NoSuchElementException("Table property is missing in data model definition")
    }

    val schema = getArrayAttribute(document, "schema") match {
      case Some(x) => x
      case None => throw new NoSuchElementException("Schema property is missing in data model definition")
    }
    val columns = columnsFromYaml(schema)

    val partitions: Option[List[Column]] = getArrayAttribute(document, "partitions") match {
      case Some(partitionColumns) => Option(columnsFromYaml(partitionColumns))
      case None => None
    }

    val literals: Option[List[LiteralColumn]] = getArrayAttribute(document, "literals") match {
      case Some(literalColumns) => Option(
        (for (i <- 0 until literalColumns.size();
              name = literalColumns.get(i).get("column").toString;
              value = literalColumns.get(i).get("value");
              literal = LiteralColumn(name, value)
              ) yield literal).toList
      )
      case None => None
    }

    new YamlSchema(table, columns, partitions, literals)
  }

  private def columnsFromYaml(columns: util.ArrayList[util.LinkedHashMap[String, AnyVal]]): List[Column] = {
    (for (i <- 0 until columns.size();
          number = requireNonNull(columns.get(i).get("number"), "Column number attribute cannot be null");
          name = requireNonNull(columns.get(i).get("column"), "Column name attribute cannot be null");
          dataType = requireNonNull(columns.get(i).get("dataType"), "Column data type attribute cannot be null");
          column = Column(
            number.asInstanceOf[Int],
            name.asInstanceOf[String],
            dataType.asInstanceOf[String])
          ) yield column).toList
  }

  def fromYamlFile(path: String): YamlSchema = {
    val schemaFile = new FileReader(path)
    fromYaml((new Yaml).load(schemaFile))
  }
}
