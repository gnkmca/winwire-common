/*
package com.winwire.adobe.execution.cassandra

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.winwire.adobe.execution.BaseTest
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar

class CassandraTest extends BaseTest with MockitoSugar with DatasetSuiteBase {
  behavior of classOf[Cassandra].getSimpleName

  private val dataframe = mock[DataFrame]
  private val writer = mock[DataFrameWriter[Row]]

  when(dataframe.write).thenReturn(writer)
  when(writer.option(any[String], any[String])).thenReturn(writer)

  class TestCassandra(sparkSession: SparkSession) extends Cassandra {
    def testIgnoreNulls: DataFrameWriter[_] = dataframe.write.ignoreNulls

    override val spark: SparkSession = sparkSession

    override protected def cassandraConfig: CassandraClusterConfig =
      CassandraClusterConfig(
        clusterName = "test",
        host = "test_host",
        port = "test_port",
        username = "test_username",
        password = "test_password",
        sslEnabled = "true",
        batchSizeRows = Some("test_batchSizeRows"),
        connectionsPerExecutorMax = Some("test_connectionsPerExecutorMax"),
        concurrentWrites = Some("test_concurrentWrites"),
        concurrentReads = Some("test_concurrentReads"),
        groupingBufferSize = Some("test_groupingBufferSize"),
        keepAliveMs = Some("test_keepAliveMs"),
        throughputMbPerSec = Some("test_throughputMbPerSec"),
        connectionFactory = Some("test_connectionFactory"),
        queryRetryCount = Some("test_queryRetryCount"),
        readsPerSec = Some("5")
      )
  }

  it should "set all needed configuration params to Spark configuration" in {
    val cassandra = new TestCassandra(spark)
    cassandra.setupCluster()

    val containsHost = spark.conf.getOption("test/spark.cassandra.connection.host").isDefined
    val containsPort = spark.conf.getOption("test/spark.cassandra.connection.port").isDefined
    val containsSsl = spark.conf.getOption("test/spark.cassandra.connection.ssl.enabled").isDefined
    val containsUsername = spark.conf.getOption("test/spark.cassandra.auth.username").isDefined
    val containsPassword = spark.conf.getOption("test/spark.cassandra.auth.password").isDefined
    val containsBatchSize = spark.conf.getOption("test/spark.cassandra.output.batch.size.rows").isDefined
    val containsConnectionsPerExecutorMax = spark.conf.getOption("test/spark.cassandra.connection.connections_per_executor_max").isDefined
    val containsConcurrentWrites = spark.conf.getOption("test/spark.cassandra.output.concurrent.writes").isDefined
    val containsConcurrentReads = spark.conf.getOption("test/spark.cassandra.concurrent.reads").isDefined
    val containsGroupingBufferSize = spark.conf.getOption("test/spark.cassandra.output.batch.grouping.buffer.size").isDefined
    val containsKeepAliveMs = spark.conf.getOption("test/spark.cassandra.connection.keep_alive_ms").isDefined
    val containsFactory = spark.conf.getOption("test/spark.cassandra.connection.factory").isDefined
    val containsThroughput = spark.conf.getOption("test/spark.cassandra.output.throughput_mb_per_sec").isDefined
    val containsQueryRetryCount = spark.conf.getOption("test/spark.cassandra.query.retry.count").isDefined
    val containsReadsPerSec = spark.conf.getOption("test/spark.cassandra.input.reads_per_sec").isDefined

    containsHost shouldBe true
    containsPort shouldBe true
    containsSsl shouldBe true
    containsUsername shouldBe true
    containsPassword shouldBe true
    containsBatchSize shouldBe true
    containsConnectionsPerExecutorMax shouldBe true
    containsConcurrentWrites shouldBe true
    containsConcurrentReads shouldBe true
    containsGroupingBufferSize shouldBe true
    containsKeepAliveMs shouldBe true
    containsFactory shouldBe true
    containsThroughput shouldBe true
    containsQueryRetryCount shouldBe true
    containsReadsPerSec shouldBe true
  }

  it should "set all settings from yaml configuration to Spark configuration" in {
    val cassandra = new TestCassandra(spark)
    cassandra.setupCluster()

    val actualHost = spark.conf.get("test/spark.cassandra.connection.host")
    val actualPort = spark.conf.get("test/spark.cassandra.connection.port")
    val actualSsl = spark.conf.get("test/spark.cassandra.connection.ssl.enabled")
    val actualUsername = spark.conf.get("test/spark.cassandra.auth.username")
    val actualPassword = spark.conf.get("test/spark.cassandra.auth.password")
    val actualBatchSize = spark.conf.get("test/spark.cassandra.output.batch.size.rows")
    val actualConnectionsPerExecutorMax = spark.conf.get("test/spark.cassandra.connection.connections_per_executor_max")
    val actualConcurrentWrites = spark.conf.get("test/spark.cassandra.output.concurrent.writes")
    val actualConcurrentReads = spark.conf.get("test/spark.cassandra.concurrent.reads")
    val actualGroupingBufferSize = spark.conf.get("test/spark.cassandra.output.batch.grouping.buffer.size")
    val actualKeepAliveMs = spark.conf.get("test/spark.cassandra.connection.keep_alive_ms")
    val factory = spark.conf.get("test/spark.cassandra.connection.factory")
    val throughput = spark.conf.get("test/spark.cassandra.output.throughput_mb_per_sec")
    val queryRetryCount = spark.conf.get("test/spark.cassandra.query.retry.count")
    val readsPerSec = spark.conf.get("test/spark.cassandra.input.reads_per_sec")

    actualHost shouldBe "test_host"
    actualPort shouldBe "test_port"
    actualSsl shouldBe "true"
    actualUsername shouldBe "test_username"
    actualPassword shouldBe "test_password"
    actualBatchSize shouldBe "test_batchSizeRows"
    actualConnectionsPerExecutorMax shouldBe "test_connectionsPerExecutorMax"
    actualConcurrentWrites shouldBe "test_concurrentWrites"
    actualConcurrentReads shouldBe "test_concurrentReads"
    actualGroupingBufferSize shouldBe "test_groupingBufferSize"
    actualKeepAliveMs shouldBe "test_keepAliveMs"
    factory shouldBe "test_connectionFactory"
    throughput shouldBe "test_throughputMbPerSec"
    queryRetryCount shouldBe "test_queryRetryCount"
    readsPerSec shouldBe "5"
  }

  it should "set ignoreNull setting when ignoreNulls method is called" in {
    val cassandra = new TestCassandra(spark)
    cassandra.setupCluster()

    cassandra.testIgnoreNulls

    verify(writer, times(1)).option("spark.cassandra.output.ignoreNulls", "true")
  }
}


 */