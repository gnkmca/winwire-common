package com.winwire.adobe.ingestion.etl.util

import org.apache.spark.sql.DataFrame

object DataframeSchemaUtils {

  def dropColumns(df: DataFrame, columns: String*): DataFrame = {
    columns.foldLeft(df) { (df, col) => df.drop(col) }
  }

  def extractTopLevelPrimitiveTypesNames(dfTypes: Array[(String, String)]): Array[String] = {
    filterDfTopLevelTypesNames(dfTypes, !_._2.startsWith("ArrayType(StructType"))
  }

  def extractTopLevelArrayTypesNames(dfTypes: Array[(String, String)]): Array[String] = {
    filterDfTopLevelTypesNames(dfTypes, _._2.startsWith("ArrayType(StructType"))
  }

  def filterDfTopLevelTypesNames(dfTypes: Array[(String, String)], predicate: ((String, String)) => Boolean): Array[String] = {
    dfTypes.filter(predicate).map(_._1)
  }
}
