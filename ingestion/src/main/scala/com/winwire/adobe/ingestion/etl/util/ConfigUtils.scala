package com.winwire.adobe.ingestion.etl.util

import com.winwire.adobe.ingestion.etl.config.AppConfig
import com.winwire.adobe.ingestion.etl.sql.TableConfig
import com.winwire.adobe.ingestion.etl.util.FileReader.read

object ConfigUtils {

  def updateTableConfig(table: TableConfig, app: AppConfig): Unit = {
    updateTableConfigs(Array(table), app)
  }

  def updateTableConfigs(tables: Array[TableConfig], config: AppConfig): Unit = {
    for (table <- tables) {
      if (table.createSqlFile != null) {
        table.createSql = read(config.app.resourcePath, table.createSqlFile)
      }
      if (table.dataSqlFile != null) {
        table.dataSql = read(config.app.resourcePath, table.dataSqlFile)
      }

      if (table.preprocessSqlFile != null) {
        table.preprocessSql = read(config.app.resourcePath, table.preprocessSqlFile)
      }
      if (table.foreachBatchSqlFile != null) {
        table.foreachBatchSql = read(config.app.resourcePath, table.foreachBatchSqlFile)
      }
      if (table.inputDataSchemaFile != null) {
        table.inputDataSchema = read(config.app.resourcePath, table.inputDataSchemaFile)
      }
      if (table.inputMetricFile != null) {
        table.inputMetricSql = read(config.app.resourcePath, table.inputMetricFile)
      }
      if (table.outputMetricFile != null) {
        table.outputMetricSql = read(config.app.resourcePath, table.outputMetricFile)
      }
    }
  }
}
