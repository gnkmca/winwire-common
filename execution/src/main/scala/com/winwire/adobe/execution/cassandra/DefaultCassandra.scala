/*
package com.winwire.adobe.execution.cassandra

import com.winwire.adobe.execution.spark.{SparkApplication, SparkConfigurator}
import org.apache.spark.sql.SparkSession

trait DefaultCassandra extends Cassandra with SparkConfigurator {
  this: SparkApplication[_ <: CassandraConfig] =>

  protected def cassandraConfig: CassandraClusterConfig = {
    val conf = config.cassandra

    CassandraClusterConfig(
      clusterName = "default",
      host = conf.host,
      port = conf.port,
      username = conf.username,
      password = conf.password,
      sslEnabled = conf.sslEnabled,
      batchSizeRows = Option(conf.batchSizeRows),
      connectionsPerExecutorMax = Option(conf.connectionsPerExecutorMax),
      concurrentWrites = Option(conf.concurrentWrites),
      concurrentReads = Option(conf.concurrentReads),
      groupingBufferSize = Option(conf.groupingBufferSize),
      keepAliveMs = Option(conf.keepAliveMs),
      throughputMbPerSec = Option(conf.throughputMbPerSec),
      connectionFactory = Option(conf.connectionFactory),
      queryRetryCount = Option(conf.queryRetryCount),
      readsPerSec = Option(conf.readsPerSec)
    )
  }

  override def configure(spark: SparkSession): Unit = {
    super.configure(spark)

    setupCluster()
  }
}
*/