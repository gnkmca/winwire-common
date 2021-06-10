package com.winwire.adobe.ingestion.etl.sql

import com.winwire.adobe.ingestion.etl.datareader.SchemaUtils

import scala.beans.BeanProperty

class TableConfig {
  @BeanProperty var db: String = _
  @BeanProperty var name: String = _
  @BeanProperty var partition: String = _
  @BeanProperty var primaryKeys: String = _
  @BeanProperty var updateDateColumn: String = _

  @BeanProperty var createSqlFile: String = _
  @BeanProperty var createSql: String = ""

  @BeanProperty var dataSqlFile: String = _
  @BeanProperty var dataSql: String = ""
  @BeanProperty var preprocessSqlFile: String = _
  @BeanProperty var preprocessSql: String = ""

  @BeanProperty var useUnion: Boolean = false

  @BeanProperty var foreachBatchSqlFile: String = _
  @BeanProperty var foreachBatchSql: String = ""

  @BeanProperty var inputDataSchemaFile: String = _
  @BeanProperty var inputDataSchema: String = ""
  @BeanProperty var inputDataSchemaFormat: String = SchemaUtils.DDL

  @BeanProperty var inputMetricFile: String = _
  @BeanProperty var inputMetricSql: String = ""

  @BeanProperty var outputMetricFile: String = _
  @BeanProperty var outputMetricSql: String = ""

  @BeanProperty var format: String = "parquet"
  @BeanProperty var saveMode: String = "append"
  @BeanProperty var dbfsPath: String = _
}
