package com.winwire.adobe.ingestion.etl.sql

import com.winwire.adobe.execution.deltalake.DeltaLake
import com.winwire.adobe.execution.spark.SparkApplication
import com.winwire.adobe.ingestion.etl.IngestionApplication
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SaveMode}

trait IngestionSql extends DeltaLake with ReadWriteUtils {
  this: SparkApplication[_] =>


  def writeTable(df: DataFrame, table: TableConfig, params: Map[String, String] = Map.empty): Unit = {
    table.saveMode match {

        // overwrite-partition works on table with one partition column
      case "overwrite-partition" | "overwrite-partition-delta" if StringUtils.isNotBlank(table.partition) =>
        if ("overwrite-partition".equals(table.saveMode)){
          executeOverwritePartitionRw(df, table)
        } else {
          executeOverwritePartitionDelta(df, table)
        }
      case "overwrite-partition" | "overwrite-partition-delta" =>
        throw new UnsupportedOperationException(s"Failed to overwrite partition without partitions")

      case "update" if StringUtils.isNotBlank(table.foreachBatchSql) =>
        executeUpdate(table, params)
      // make sure that your data deduped by primaryKeys
      case "update" if StringUtils.isNotBlank(table.primaryKeys) =>
        executeMerge(df, table)
      case "update" =>
        throw new UnsupportedOperationException(s"Failed to update without query or primary-keys")

      case "append" | "overwrite" =>
        val mode = SaveMode.valueOf(table.saveMode.capitalize)
        table.format match {
          case "delta" =>
            writeToDelta(df, table.dbfsPath, mode, Option(table.partition))
          case _ =>
            logger.info(s"Write to ${table.format}[${table.saveMode}]: ${table.db}.${table.name}: ${table.dbfsPath}")
            val writer = df.write
              .format(table.format)
              .mode(mode)
            if (table.partition != null) {
              writer.partitionBy(table.partition)
            }
            writer.save(table.dbfsPath)
        }
      case _ => throw new UnsupportedOperationException(s"Unsupported save mode for batch df: ${table.saveMode}")
    }

  }

  def writeStream(df: DataFrame, table: TableConfig, checkpoint: String,
                  params: Map[String, String] = Map.empty): StreamingQuery = {
    val writer = df.writeStream
      .queryName(table.name)
      .trigger(Trigger.Once())
      .format(table.format)
      .option("checkpointLocation", checkpoint)

    if (StringUtils.isBlank(table.foreachBatchSql)) {
      logger.info(s"Execute ${table.saveMode} for streaming df")
      writer
        .option("path", table.dbfsPath)
        .outputMode(table.saveMode)
    } else {
      logger.info(s"Execute ${table.saveMode} for streaming df with foreach batch sql")
      writer
        .outputMode("update")
        .foreachBatch((batchDf: DataFrame, _: Long) => {
          batchDf.createOrReplaceTempView(params(IngestionApplication.OUTPUT_VIEW_KEY))
          executeSql(table.foreachBatchSql, params, Option(batchDf.sparkSession))
        })
    }

    writer.start()
  }

  protected def dropDuplicates(df: DataFrame, table: TableConfig): DataFrame = {
    val data = if (table.useUnion) {
      df.unionByName(readTable(table))
    } else {
      df
    }

    val primaryKeys = table.primaryKeys.split(",").map(_.trim)
    val windowSpec = Window
      .partitionBy(primaryKeys.map(col): _*)
      .orderBy(desc(table.updateDateColumn))

    data
      .select(
        col("*"),
        row_number().over(windowSpec).as("__rank__")
      )
      .filter(col("__rank__") === 1)
      .drop(col("__rank__"))
  }

}
