package com.winwire.adobe.ingestion.etl.config

import com.winwire.adobe.ingestion.etl.sql.TableConfig

import scala.beans.BeanProperty

class InternalWithExtnApplicationConfig[T <: TableConfig] extends InternalApplicationConfig[T] {
  @BeanProperty var extnTable: T = _
}
