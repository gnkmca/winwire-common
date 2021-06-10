package com.winwire.adobe.ingestion.etl.sql

import com.winwire.adobe.execution.spark.SparkApplication
import com.winwire.adobe.ingestion.etl.util.Params.insertParams
import org.apache.commons.lang3.StringUtils

trait TableCreator {
  this: SparkApplication[_] =>

  def createTables(tables: Array[TableConfig]): Unit = {
    for (table <- tables) {
      createTable(table)
    }
  }

  def createTable(table: TableConfig): Unit = {
    val params = Map(
      "db" -> table.db,
      "table" -> table.name,
      "format" -> table.format,
      "path" -> table.dbfsPath
    )
    val sql = insertParams(table.createSql, params)
    val queries = sql.split(";").filter(s => StringUtils.isNotBlank(s))
    for (q <- queries) {
      logger.info(s"Execute create table sql: $q")
      spark.sql(q)
    }
  }
}
