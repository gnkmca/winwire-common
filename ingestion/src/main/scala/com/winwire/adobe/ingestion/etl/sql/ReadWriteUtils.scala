package com.winwire.adobe.ingestion.etl.sql

import com.winwire.adobe.execution.deltalake.DeltaLake
import com.winwire.adobe.execution.spark.SparkApplication
import com.winwire.adobe.ingestion.etl.util.Params.insertParams
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

trait ReadWriteUtils extends DeltaLake {
  this: SparkApplication[_] =>

  import spark.implicits._

  def executeUpdate(table: TableConfig, params: Map[String, String] = Map.empty): Unit = {
    logger.info("Use query to update table")
    executeSql(table.foreachBatchSql, params)
  }

  def executeOverwritePartitionDelta(df: DataFrame, table: TableConfig): Unit = {
    logger.info("Execute overwrite partitions via Delta")

    df.persist(StorageLevel.MEMORY_AND_DISK_2)

    val partitionValues = getPartitionValues(df, table)
    logger.info(s"Partitions to update ${partitionValues.mkString("Array(", ", ", ")")}")

    val version = getLastDeltaVersion(table)
    logger.info(s"Current delta version: $version")

    executeOverwritePartitionDelta(df, table, partitionValues) match {
      case Success(_) =>
      case Failure(exception) =>
        logger.error(s"Exception occurred during writing: $exception")
        rollbackTable(table, version)
        throw exception
    }

    df.unpersist()
  }

  protected def rollbackTable(table: TableConfig, version: Option[Long]): Unit = {
    logger.info(s"Rollback to version: $version")
    Try(executeSql(s"RESTORE TABLE ${table.db}.${table.name} TO VERSION AS OF $version")) match {
      case Success(_) =>
        logger.info(s"Rolled back to version[$version] successfully")
      case _ =>
        logger.error("Failed to RESTORE TABLE. Check databricks runtime version")
        logger.info("Use spark write to rollback version")
        writeToDelta(readTable(table, version), table.dbfsPath, SaveMode.Overwrite, Option(table.partition))
    }
  }

  private def getPartitionValues(input: DataFrame, table: TableConfig): Array[Any] = {
    input
      .select(table.partition)
      .distinct()
      .filter(col(table.partition).isNotNull)
      .collect()
      .map(r => r.get(0))
  }

  protected def getLastDeltaVersion(table: TableConfig): Option[Long] = {

    if (tableExists(table.dbfsPath)) {
      val value = executeSql(
        s"""
           |SELECT max(version)
           |FROM (DESCRIBE HISTORY delta.`${table.dbfsPath}`)""".stripMargin
      )
      value.as[Long].take(1).headOption.flatMap(Option(_))
    } else {
      None
    }
  }

  private def executeOverwritePartitionDelta(df: DataFrame, table: TableConfig, partitionValues: Seq[Any]): Try[Unit] = Try({
    partitionValues.foreach(v => {
      logger.info(s"Delete partition[${table.partition}] = $v")
      deleteFromDelta(table.dbfsPath, Map(table.partition -> v))
    })
    logger.info(s"Write to ${table.format}[${SaveMode.Append}]: ${table.db}.${table.name}: ${table.dbfsPath}")
    writeToDelta(df, table.dbfsPath, SaveMode.Append, Option(table.partition))
  })

  def executeOverwritePartitionRw(input: DataFrame, table: TableConfig): Unit = {
    logger.info("Execute overwrite partitions via replaceWhere")

    val partitionValues = getPartitionValues(input, table)
      .mkString("'", "', '", "'")

    logger.info(s"Partitions to update $partitionValues")
    input.write
      .format(table.format)
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"${table.partition} in ($partitionValues)")
      .save(table.dbfsPath)
  }

  def readTable(table: TableConfig, version: Option[Long] = None): DataFrame = {
    val t = version match {
      case Some(v) => s"${table.db}.${table.name}@v$v"
      case _ => s"${table.db}.${table.name}"
    }
    logger.info(s"Execute read table: $t")
    spark.read.table(t)
  }

  def executeMerge(df: DataFrame, table: TableConfig): Unit = {
    table.format match {
      case "delta" =>
        logger.info(s"Execute [${table.format}] merge with primary keys[${table.primaryKeys}]")
        val keys = table.primaryKeys.split(",").filter(_.nonEmpty)
        merge(df, table.dbfsPath, keys, Option(table.partition))
      case _ =>
        throw new UnsupportedOperationException(s"Failed to merge [${table.format}] with primary keys[${table.primaryKeys}]")
    }
  }

  def getMaxUpdateDateColumnValue(table: TableConfig): Option[String] = {
    if (spark.catalog.tableExists(table.db, table.name)) {
      val value = executeSql(
        s"""
           |SELECT max(${table.updateDateColumn})
           |FROM ${table.db}.${table.name}""".stripMargin
      )
      value.as[String].take(1).headOption.flatMap(Option(_))
    } else {
      None
    }
  }

  def executeSql(query: String, params: Map[String, String] = Map.empty, sparkO: Option[SparkSession] = None): DataFrame = {
    val ss = sparkO.getOrElse(spark)
    val sql = insertParams(query, params)
    logger.info(s"Execute sql: $sql")
    ss.sql(sql)
  }
}
