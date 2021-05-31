package com.winwire.adobe.common.execution.cassandra

final case class CassandraClusterConfig(clusterName: String,
                                        host: String,
                                        port: String,
                                        username: String,
                                        password: String,
                                        sslEnabled: String,
                                        batchSizeRows: Option[String] = None,
                                        connectionsPerExecutorMax: Option[String] = None,
                                        concurrentWrites: Option[String] = None,
                                        concurrentReads: Option[String] = None,
                                        groupingBufferSize: Option[String] = None,
                                        keepAliveMs: Option[String] = None,
                                        throughputMbPerSec: Option[String] = None,
                                        connectionFactory: Option[String] = None,
                                        queryRetryCount: Option[String] = None,
                                        readsPerSec: Option[String] = None
                                       )
