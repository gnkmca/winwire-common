/*package com.winwire.adobe.common.execution.cassandra

import java.net.InetAddress

import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, PasswordAuthConf}
import com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory
import com.winwire.adobe.common.execution.Configurator
import com.winwire.adobe.common.execution.spark.ColumnNamesConverter._
import com.winwire.adobe.common.execution.spark.Spark
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

trait Cassandra extends Spark with Configurator {

  protected def cassandraConfig: CassandraClusterConfig

  private[this] lazy val conf = cassandraConfig

  def setupCluster(): Unit = {
    implicit val clusterConf: mutable.Map[String, String] = mutable.Map(
      "spark.cassandra.connection.host" -> conf.host,
      "spark.cassandra.connection.port" -> conf.port,
      "spark.cassandra.auth.username" -> conf.username,
      "spark.cassandra.auth.password" -> conf.password,
      "spark.cassandra.connection.ssl.enabled" -> conf.sslEnabled
    )
    setIfNotEmpty("spark.cassandra.output.batch.size.rows", conf.batchSizeRows)
    setIfNotEmpty("spark.cassandra.connection.connections_per_executor_max", conf.connectionsPerExecutorMax)
    setIfNotEmpty("spark.cassandra.output.concurrent.writes", conf.concurrentWrites)
    setIfNotEmpty("spark.cassandra.concurrent.reads", conf.concurrentReads)
    setIfNotEmpty("spark.cassandra.output.batch.grouping.buffer.size", conf.groupingBufferSize)
    setIfNotEmpty("spark.cassandra.connection.keep_alive_ms", conf.keepAliveMs)
    setIfNotEmpty("spark.cassandra.output.throughput_mb_per_sec", conf.throughputMbPerSec)
    setIfNotEmpty("spark.cassandra.query.retry.count", conf.queryRetryCount)
    setIfNotEmpty("spark.cassandra.input.reads_per_sec", conf.readsPerSec)

    spark.setCassandraConf(conf.clusterName, clusterConf.toMap)

    if (conf.connectionFactory.isDefined) {
      spark.conf.set(s"${conf.clusterName}/spark.cassandra.connection.factory", conf.connectionFactory.get)
    }
  }

  def readFromCassandra[A <: Product : TypeTag](keyspace: String, table: String): Dataset[A] = {
    spark.read
      .cassandraFormat(table, keyspace, conf.clusterName)
      .load()
      .toCamel
      .as[A](Encoders.product[A])
  }

  def writeToCassandra[A](ds: Dataset[A], keyspace: String, table: String): Unit =
    writeToCassandra(ds, keyspace, table, SaveMode.Append)

  def writeToCassandra[A](ds: Dataset[A], keyspace: String, table: String,
                          saveMode: SaveMode): Unit = {

    ds.toDF().toSnake
      .write
      .ignoreNulls
      .cassandraFormat(table, keyspace, conf.clusterName)
      .mode(saveMode)
      .save()
  }

  private def cassandraConnector: CassandraConnector = CassandraConnector(
    hosts = Set(InetAddress.getByName(conf.host)),
    port = Try(conf.port.toInt).getOrElse(CassandraConnectorConf.ConnectionPortParam.default),
    authConf = PasswordAuthConf(conf.username, conf.password),
    keepAliveMillis = Try(conf.keepAliveMs.getOrElse("").toInt)
      .getOrElse(CassandraConnectorConf.KeepAliveMillisParam.default),
    connectionFactory = CosmosDbConnectionFactory,
    cassandraSSLConf = CassandraSSLConf(enabled = Try(conf.sslEnabled.toBoolean).getOrElse(false))
  )

  def executeCql(cql: String): Unit = {
    cassandraConnector.withSessionDo(session =>
      session.execute(cql)
    )
  }

  implicit class CassandraDataset[T](ds: Dataset[T]) {
    def foreachRowCql(cql: T => String): Unit = {
      val connector: CassandraConnector = cassandraConnector

      ds.foreachPartition(partition => {
        connector.withSessionDo(session =>
          partition.foreach { row =>
            session.execute(cql(row))
          })
      })
    }
  }

  implicit class CassandraDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def ignoreNulls: DataFrameWriter[T] = {
      writer.option("spark.cassandra.output.ignoreNulls", "true")
    }
  }

}

object Cassandra {
  def apply(config: CassandraClusterConfig)(implicit ss: SparkSession): Cassandra = {
    val cassandra = new Cassandra {
      val spark: SparkSession = ss

      protected def cassandraConfig: CassandraClusterConfig = config
    }
    cassandra.setupCluster()
    cassandra
  }
}*/