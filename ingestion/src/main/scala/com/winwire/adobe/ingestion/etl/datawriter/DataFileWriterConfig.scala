package com.winwire.adobe.ingestion.etl.datawriter

import com.winwire.adobe.ingestion.etl.util.KeyValue

import scala.beans.BeanProperty

trait DataFileWriterConfig {

  @BeanProperty var rawDataFormat: String = "csv"
  @BeanProperty var writeOptions: Array[KeyValue] = Array()

  def writeOptionsAsMap: Map[String, String] = {
    writeOptions
      .map(kv => (kv.key, kv.value))
      .toMap
  }
}