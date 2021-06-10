/*
package com.winwire.adobe.execution.cassandra

import java.net.URL

import com.winwire.adobe.config.{ContainsSecrets, SecretsStorage, SecretsStorageProvider}
import com.winwire.adobe.execution.spark.SparkApplication
import com.winwire.adobe.execution.{BaseTest, SparkSessionProviderForTest}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar

import scala.beans.BeanProperty
import scala.reflect.{ClassTag, classTag}

class CassandraTestConfig extends CassandraConfig with ContainsSecrets {
  @BeanProperty var test: String = _
  @BeanProperty var testInt: Int = _
}

trait TestSecretsStorageProvider extends SecretsStorageProvider {
  override def secretsStorage[A](config: => A): SecretsStorage = new TestSecretsStorage()
}

class TestSecretsStorage extends SecretsStorage {
  override def get(name: String): Option[String] = Some(name)
}

class DefaultCassandraTest extends BaseTest with MockitoSugar {
  behavior of classOf[DefaultCassandra].getSimpleName

  private val dataframe = mock[DataFrame]
  private val writer = mock[DataFrameWriter[Row]]

  when(dataframe.write).thenReturn(writer)
  when(writer.option(any[String], any[String])).thenReturn(writer)

  class TestCassandraApplication extends SparkApplication[CassandraTestConfig]
    with DefaultCassandra with SparkSessionProviderForTest with TestSecretsStorageProvider {
    override val configTag: ClassTag[CassandraTestConfig] = classTag[CassandraTestConfig]

    override def name = "cassandra"

    override def script(): Unit = {}

    def testIgnoreNulls: DataFrameWriter[_] = dataframe.write.ignoreNulls
  }

  it should "set all needed configuration params to Spark configuration" in {
    val app = createAppAndRun()

    val containsHost = app.spark.conf.getOption("default/spark.cassandra.connection.host").isDefined
    val containsPort = app.spark.conf.getOption("default/spark.cassandra.connection.port").isDefined
    val containsSsl = app.spark.conf.getOption("default/spark.cassandra.connection.ssl.enabled").isDefined
    val containsUsername = app.spark.conf.getOption("default/spark.cassandra.auth.username").isDefined
    val containsPassword = app.spark.conf.getOption("default/spark.cassandra.auth.password").isDefined
    val containsBatchSize = app.spark.conf.getOption("default/spark.cassandra.output.batch.size.rows").isDefined
    val containsConnectionsPerExecutorMax = app.spark.conf.getOption("default/spark.cassandra.connection.connections_per_executor_max").isDefined
    val containsConcurrentWrites = app.spark.conf.getOption("default/spark.cassandra.output.concurrent.writes").isDefined
    val containsConcurrentReads = app.spark.conf.getOption("default/spark.cassandra.concurrent.reads").isDefined
    val containsGroupingBufferSize = app.spark.conf.getOption("default/spark.cassandra.output.batch.grouping.buffer.size").isDefined
    val containsKeepAliveMs = app.spark.conf.getOption("default/spark.cassandra.connection.keep_alive_ms").isDefined
    val containsFactory = app.spark.conf.getOption("default/spark.cassandra.connection.factory").isDefined
    val containsThroughput = app.spark.conf.getOption("default/spark.cassandra.output.throughput_mb_per_sec").isDefined
    val containsQueryRetryCount = app.spark.conf.getOption("default/spark.cassandra.query.retry.count").isDefined
    val containsReadsPerSec = app.spark.conf.getOption("default/spark.cassandra.input.reads_per_sec").isDefined

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
    val app = createAppAndRun()

    val actualHost = app.spark.conf.get("default/spark.cassandra.connection.host")
    val actualPort = app.spark.conf.get("default/spark.cassandra.connection.port")
    val actualSsl = app.spark.conf.get("default/spark.cassandra.connection.ssl.enabled")
    val actualUsername = app.spark.conf.get("default/spark.cassandra.auth.username")
    val actualPassword = app.spark.conf.get("default/spark.cassandra.auth.password")
    val actualBatchSize = app.spark.conf.get("default/spark.cassandra.output.batch.size.rows")
    val actualConnectionsPerExecutorMax = app.spark.conf.get("default/spark.cassandra.connection.connections_per_executor_max")
    val actualConcurrentWrites = app.spark.conf.get("default/spark.cassandra.output.concurrent.writes")
    val actualConcurrentReads = app.spark.conf.get("default/spark.cassandra.concurrent.reads")
    val actualGroupingBufferSize = app.spark.conf.get("default/spark.cassandra.output.batch.grouping.buffer.size")
    val actualKeepAliveMs = app.spark.conf.get("default/spark.cassandra.connection.keep_alive_ms")
    val factory = app.spark.conf.get("default/spark.cassandra.connection.factory")
    val throughput = app.spark.conf.get("default/spark.cassandra.output.throughput_mb_per_sec")
    val queryRetryCount = app.spark.conf.get("default/spark.cassandra.query.retry.count")
    val readsPerSec = app.spark.conf.get("default/spark.cassandra.input.reads_per_sec")

    actualHost shouldBe "test_host"
    actualPort shouldBe "9091"
    actualSsl shouldBe "true"
    actualUsername shouldBe "test_username"
    actualPassword shouldBe "test_password"
    actualBatchSize shouldBe "200"
    actualConnectionsPerExecutorMax shouldBe "1"
    actualConcurrentWrites shouldBe "10"
    actualConcurrentReads shouldBe "256"
    actualGroupingBufferSize shouldBe "500"
    actualKeepAliveMs shouldBe "10000"
    factory shouldBe "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory"
    throughput shouldBe "1"
    queryRetryCount shouldBe "1000"
    readsPerSec shouldBe "10"
  }

  private def createAppAndRun() = {
    val url: URL = getClass.getClassLoader.getResource("cassandra_test.yaml")
    val app = new TestCassandraApplication
    app.main(Array("--config", url.getPath))
    app
  }
}


 */