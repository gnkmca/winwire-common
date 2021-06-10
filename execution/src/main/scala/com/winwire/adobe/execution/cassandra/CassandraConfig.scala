package com.winwire.adobe.execution.cassandra

import scala.beans.BeanProperty

class CassandraSection {
  @BeanProperty var host: String = _
  @BeanProperty var port: String = _
  @BeanProperty var username: String = _
  @BeanProperty var password: String = _
  @BeanProperty var sslEnabled: String = _
  @BeanProperty var batchSizeRows: String = _
  @BeanProperty var connectionsPerExecutorMax: String = _
  @BeanProperty var concurrentWrites: String = _
  @BeanProperty var concurrentReads: String = _
  @BeanProperty var groupingBufferSize: String = _
  @BeanProperty var keepAliveMs: String = _
  @BeanProperty var throughputMbPerSec: String = _
  @BeanProperty var connectionFactory: String = "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory"
  @BeanProperty var queryRetryCount: String = _
  @BeanProperty var readsPerSec: String = _
}

trait CassandraConfig {
  @BeanProperty var cassandra: CassandraSection = _
}
