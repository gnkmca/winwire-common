package com.winwire.adobe.execution.spark
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import org.apache.spark.sql.{DataFrame, Dataset}

trait SchemaMapper {
  def updateSchema[T](ds: Dataset[T], mappings: Map[String, String]): DataFrame = {
    val df = ds.toDF

    df.columns.foldLeft(df)((df, fromColumn) => {
      mappings.get(fromColumn) match {
        case Some(toColumn) => df.withColumnRenamed(fromColumn, toColumn)
        case None => df
      }
    })
  }
}
