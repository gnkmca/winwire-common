package com.winwire.adobe.common.execution.kafka

import scala.beans.BeanProperty

class KafkaSection {
  // Format: "host:port, host1:port1"
  @BeanProperty var bootstrapServers: String = _
  @BeanProperty var startingOffsets: String = "latest"
  @BeanProperty var failOnDataLoss: Boolean = true
  @BeanProperty var kafkaSecurityProtocol: String = "PLAINTEXT"
}

trait KafkaConfig {
  @BeanProperty var kafka: KafkaSection = _
}
