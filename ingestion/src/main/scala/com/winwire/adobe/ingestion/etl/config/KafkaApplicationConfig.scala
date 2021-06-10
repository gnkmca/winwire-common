/*package com.winwire.adobe.ingestion.etl.config

import com.winwire.adobe.execution.kafka.KafkaConfig
import com.winwire.adobe.ingestion.etl.datareader.DataFileReaderConfig
import com.winwire.adobe.ingestion.etl.sql.TableConfig
import com.winwire.adobe.ingestion.etl.util.KeyValue

import scala.beans.BeanProperty


class KafkaTableConfig extends TableConfig with DataFileReaderConfig {
  @BeanProperty var dbfsCheckpointLocation: String = _
}

class KafkaApplicationConfig[T <: KafkaTableConfig] extends AppConfig with KafkaConfig {
  @BeanProperty var topic: String = _
  @BeanProperty var tables: Array[T] = _
}


 */