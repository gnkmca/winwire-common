package com.winwire.adobe.common.execution.spark

import org.apache.spark.sql.DataFrame

object ColumnNamesConverter {

  implicit class Conversions(val df: DataFrame) {
    def toCamel: DataFrame = {
      df.columns.foldLeft(df)((df, colName) =>
        df.withColumnRenamed(colName, snakeToCamel(colName)))
    }

    def toSnake: DataFrame = {
      df.columns.foldLeft(df)((df, colName) =>
        df.withColumnRenamed(colName, camelToSnake(colName)))
    }

    private def camelToSnake(name: String): String = "[A-Z]".r.replaceAllIn(name, { m =>
      "_" + m.group(0).toLowerCase()
    })

    private def snakeToCamel(name: String): String = "_([a-z\\d])".r.replaceAllIn(name, { m =>
      m.group(1).toUpperCase()
    })
  }
}
