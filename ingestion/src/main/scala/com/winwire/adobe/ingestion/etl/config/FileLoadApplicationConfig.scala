package com.winwire.adobe.ingestion.etl.config

import com.winwire.adobe.ingestion.etl.datareader.DataFileReaderConfig
import com.winwire.adobe.ingestion.etl.sql.TableConfig

import scala.beans.BeanProperty

class FileLoadTableConfig extends TableConfig with DataFileReaderConfig {
  @BeanProperty var schemaFromTargetTable: Boolean = false
  @BeanProperty var rawDataPath: String = _
  @BeanProperty var resolveSubdirs: Boolean = false
  @BeanProperty var dateTimeFilter: DateTimeFilter = _
}


class FileLoadApplicationConfig[T <: FileLoadTableConfig] extends AppConfig {
  @BeanProperty var table: T = _
}

class DateTimeFilter {
  @BeanProperty var firstDate: String = _
  @BeanProperty var minChronoUnit: String = _
  @BeanProperty var maxDuration: Int = -1
  @BeanProperty var dateTimePathFormat: String = _
  @BeanProperty var inclusive: Boolean = false
}