package com.winwire.adobe.execution.deltalake

import io.delta.tables.DeltaTable
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

trait DeltaLake {

  def readDfFromDelta(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("delta")
      .load(path)
  }

  def readFromDelta[A <: Product : TypeTag](path: String)
                                           (implicit spark: SparkSession): Dataset[A] = {
    readDfFromDelta(path)
      .as[A](Encoders.product[A])
  }

  def writeToDelta[A](ds: Dataset[A], path: String,
                      partitionBy: Option[String] = None): Unit =
    writeToDelta(ds, path, SaveMode.Append, partitionBy)

  def writeToDelta[A](ds: Dataset[A], path: String, mode: SaveMode,
                      partitionBy: Option[String]): Unit = {
    val delta = ds.write
      .format("delta")
      .mode(mode)
      .option("mergeSchema", "true")

    (partitionBy match {
      case Some(v) if StringUtils.isNotBlank(v) =>
        val partitionCol = v.split(",").map(_.trim).filter(_.nonEmpty)
        delta.partitionBy(partitionCol: _*)
      case _ => delta
    }).save(path)
  }

  def writeStreamToDelta[A](ds: Dataset[A], path: String,
                            checkpointLocation: String, mode: String = "append"): Unit = {
    ds.writeStream
      .format("delta")
      .outputMode(mode)
      .option("checkpointLocation", checkpointLocation)
      .option("mergeSchema", "true")
      .start(path)
      .awaitTermination()
  }

  def merge[T](newData: Dataset[T], tablePath: String, columns: Seq[String], partitionBy: Option[String] = None)
              (implicit spark: SparkSession): Unit = {
    if (!tableExists(tablePath)) {
      writeToDelta(newData, tablePath, partitionBy)
    }
    else {
      merge(newData, DeltaTable.forPath(tablePath), columns)
    }
  }

  def merge[T](newData: Dataset[T], table: DeltaTable, column: String): Unit =
    merge(newData, table, Seq(column))

  def merge[T](newData: Dataset[T], tablePath: String, column: String, partitionBy: Option[String])
              (implicit spark: SparkSession): Unit =
    merge(newData, tablePath, Seq(column), partitionBy)

  def merge[T](newData: Dataset[T], table: DeltaTable, columns: Seq[String]): Unit = {
    val oldTableName = "old"
    val newTableName = "new"
    val condition = getMergeCondition(columns, oldTableName, newTableName)

    table.as("old")
      .merge(newData.toDF.as("new"), condition)
      .whenMatched
      .updateAll
      .whenNotMatched
      .insertAll
      .execute()
  }

  def getMergeCondition(columns: Seq[String], oldTableName: String, newTableName: String): String = {
    columns
      .map(column => s"$oldTableName.$column = $newTableName.$column")
      .mkString(" and ")
  }

  def deleteFromDelta(tablePath: String, conditions: Map[String, Any]): Unit =
    deleteFromDelta(DeltaTable.forPath(tablePath), conditions)

  def deleteFromDelta(table: DeltaTable, conditions: Map[String, Any]): Unit = {
    val condition = conditions.map {
      case (column, value) => s"$column = $value"
    }.mkString(" and ")

    table.delete(condition)
  }

  def tableExists(path: String)(implicit spark: SparkSession): Boolean =
    DeltaTable.isDeltaTable(spark, path)
}

object DeltaLake {
  def apply(): DeltaLake = new DeltaLake {}
}