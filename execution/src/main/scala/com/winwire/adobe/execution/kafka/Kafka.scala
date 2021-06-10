package com.winwire.adobe.execution.kafka

import java.sql.Timestamp

import com.winwire.adobe.execution.spark.Spark
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait Kafka extends Spark {
  protected def kafkaConfig: KafkaClusterConfig

  private[this] lazy val conf = kafkaConfig

  import spark.implicits._

  def readKafkaStream(topic: String): Dataset[KafkaRecord] =
    readKafkaStream(Seq(topic))

  def readKafkaStream(topics: Seq[String]): Dataset[KafkaRecord] = {
    val topicsStr = topics.mkString(",")

    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.bootstrapServers)
      .option("subscribe", topicsStr)
      .option("startingOffsets", conf.startingOffsets)
      .option("failOnDataLoss", conf.failOnDataLoss)
      .option("kafka.security.protocol", conf.kafkaSecurityProtocol)
      .load()

    kafkaDatasetFrom(source)
  }

  def createStreamToKafka(ds: Dataset[_], topic: String, checkpointLocation: String): StreamingQuery =
    createStreamToKafka(ds, topic, checkpointLocation, None)

  def createStreamToKafka(ds: Dataset[_], topic: String, checkpointLocation: String,
                          keyColumn: Option[String]): StreamingQuery = {
    kafkaDatasetToWrite(ds, keyColumn)
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.bootstrapServers)
      .option("kafka.security.protocol", conf.kafkaSecurityProtocol)
      .option("topic", topic)
      .option("checkpointLocation", checkpointLocation)
      .start()
  }

  def writeStreamToKafka(ds: Dataset[_], topic: String, checkpointLocation: String): Unit =
    writeStreamToKafka(ds, topic, checkpointLocation, None)

  def writeStreamToKafka(ds: Dataset[_], topic: String, checkpointLocation: String,
                         keyColumn: Option[String]): Unit = {
    val query = createStreamToKafka(ds, topic, checkpointLocation, keyColumn)

    query.awaitTermination()
  }

  def writeToKafka(ds: Dataset[_], topic: String): Unit =
    writeToKafka(ds, topic, None)

  def writeToKafka(ds: Dataset[_], topic: String, keyColumn: Option[String]): Unit = {
    kafkaDatasetToWrite(ds, keyColumn)
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.bootstrapServers)
      .option("kafka.security.protocol", conf.kafkaSecurityProtocol)
      .option("topic", topic)
      .save()
  }

  private[kafka] def kafkaDatasetToWrite[A](source: Dataset[A],
                                            keyColumn: Option[String]): Dataset[KafkaWriteRecord] = {
    val key = keyColumn.getOrElse("null")
    val keyExpression = s"CAST($key AS STRING) AS key"

    source.selectExpr(keyExpression, "to_json(struct(*)) AS value")
      .as[KafkaWriteRecord]
  }

  private[kafka] def kafkaDatasetFrom(source: DataFrame) = {
    source.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .as[KafkaRecord]
  }
}

object Kafka {
  def apply(config: KafkaClusterConfig)(implicit ss: SparkSession): Kafka =
    new Kafka {
      val spark: SparkSession = ss

      protected def kafkaConfig: KafkaClusterConfig = config
    }
}

case class KafkaRecord(
                        key: String,
                        value: String,
                        timestamp: Timestamp
                      )

case class KafkaWriteRecord(key: String, value: String)