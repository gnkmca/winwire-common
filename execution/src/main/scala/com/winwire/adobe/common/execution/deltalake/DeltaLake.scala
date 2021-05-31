/*package com.winwire.adobe.common.execution.deltalake

import io.delta.tables.DeltaTable
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
    var delta = ds.write
      .format("delta")
      .mode(mode)
      .option("mergeSchema", "true")

    if (partitionBy.isDefined) {
      delta = delta.partitionBy(partitionBy.get)
    }

    delta.save(path)
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

  def merge[T](newData: Dataset[T], tablePath: String, column: String)
              (implicit spark: SparkSession): Unit = {
    if (!tableExists(tablePath)) {
      writeToDelta(newData, tablePath)
    }
    else {
      merge(newData, DeltaTable.forPath(tablePath), column)
    }
  }

  def merge[T](newData: Dataset[T], table: DeltaTable, column: String): Unit =
    merge(newData, table, Seq(column))

  def merge[T](newData: Dataset[T], table: DeltaTable, columns: Seq[String]): Unit = {
    val condition = columns
      .map(column => s"old.$column = new.$column")
      .mkString(" and ")

    table.as("old")
      .merge(newData.toDF.as("new"), condition)
      .whenMatched
      .updateAll
      .whenNotMatched
      .insertAll
      .execute()
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
}*/