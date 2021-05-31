package com.winwire.adobe.common.execution.kafka

case class KafkaClusterConfig(
                               bootstrapServers: String,
                               startingOffsets: String = "latest",
                               failOnDataLoss: Boolean = true,
                               kafkaSecurityProtocol: String
                             )
