package com.winwire.adobe.ingestion.etl.config

import com.winwire.adobe.ingestion.etl.sql.TableConfig

import scala.beans.BeanProperty


class InternalApplicationConfig[T <: TableConfig] extends AppConfig {
  @BeanProperty var table: T = _
}