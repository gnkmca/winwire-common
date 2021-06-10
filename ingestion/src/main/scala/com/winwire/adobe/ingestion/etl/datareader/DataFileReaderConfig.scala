package com.winwire.adobe.ingestion.etl.datareader

import com.winwire.adobe.ingestion.etl.util.KeyValue

import scala.beans.BeanProperty

trait DataFileReaderConfig {
  @BeanProperty var rawDataFormat: String = "orc"
  @BeanProperty var readOptions: Array[KeyValue] = Array()

  def readOptionsAsMap: Map[String, String] = {
    readOptions
      .map(kv => (kv.key, kv.value))
      .toMap
  }
}