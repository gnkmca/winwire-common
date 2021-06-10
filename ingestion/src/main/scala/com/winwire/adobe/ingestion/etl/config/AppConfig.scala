package com.winwire.adobe.ingestion.etl.config

import com.winwire.adobe.ingestion.etl.util.KeyValue

import scala.beans.BeanProperty


trait AppConfig extends MetastoreConfig {
  @BeanProperty var jobName: String = _
  @BeanProperty var app: AppConfigSelection = _
}

class AppConfigSelection {
  @BeanProperty var resourcePath: String = _
  @BeanProperty var isHistorical: Boolean = false
  @BeanProperty var options: Array[KeyValue] = Array.empty
  @BeanProperty var sparkConf: Array[KeyValue] = Array.empty
  @BeanProperty var hadoopConf: Array[KeyValue] = Array.empty

  def optionsAsMap: Map[String, String] = {
    Map[String, String](
      options
        .map(kv => (kv.key, kv.value))
        .toSeq: _*
    )
  }
}