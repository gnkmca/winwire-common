package com.winwire.adobe.execution.kafka

import com.winwire.adobe.execution.spark.SparkApplication

trait DefaultKafka extends Kafka {
  this: SparkApplication[_ <: KafkaConfig] =>

  protected def kafkaConfig: KafkaClusterConfig = {
    val conf = config.kafka

    KafkaClusterConfig(
      bootstrapServers = conf.bootstrapServers,
      startingOffsets = conf.startingOffsets,
      failOnDataLoss = conf.failOnDataLoss,
      kafkaSecurityProtocol = conf.kafkaSecurityProtocol
    )
  }
}
