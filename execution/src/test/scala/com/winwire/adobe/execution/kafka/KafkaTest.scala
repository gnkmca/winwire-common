/*
package com.winwire.adobe.execution.kafka

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.winwire.adobe.execution.BaseTest
import com.winwire.adobe.execution.spark.{SparkApplication, SparkSessionProvider}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, SparkSession}
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar

class KafkaTest extends BaseTest
  with MockitoSugar
  with DatasetSuiteBase {

  behavior of classOf[Kafka].getSimpleName

  private val streamReader = mock[DataStreamReader]
  private val streamWriter = mock[DataStreamWriter[KafkaWriteRecord]]
  private val dfWriter = mock[DataFrameWriter[KafkaWriteRecord]]
  private val sparkMock = mock[SparkSession]
  private val sourceDf = mock[DataFrame]
  private val kafkaDs = mock[Dataset[KafkaRecord]]
  private val kafkaWriteDs = mock[Dataset[KafkaWriteRecord]]
  private val streamingQuery = mock[StreamingQuery]

  when(streamReader.format(any[String])).thenReturn(streamReader)
  when(streamReader.option(any[String], any[String])).thenReturn(streamReader)
  when(streamReader.option(any[String], any[Boolean])).thenReturn(streamReader)
  when(streamReader.load()).thenReturn(sourceDf)
  when(sparkMock.readStream).thenReturn(streamReader)

  when(streamWriter.format(any[String])).thenReturn(streamWriter)
  when(streamWriter.option(any[String], any[String])).thenReturn(streamWriter)
  when(streamWriter.start).thenReturn(streamingQuery)
  when(kafkaWriteDs.writeStream).thenReturn(streamWriter)

  when(dfWriter.format(any[String])).thenReturn(dfWriter)
  when(dfWriter.option(any[String], any[String])).thenReturn(dfWriter)
  when(kafkaWriteDs.write).thenReturn(dfWriter)

  class TestConfig extends KafkaConfig

  trait MockSparkSessionProvider extends SparkSessionProvider {
    //noinspection SameParameterValue
    override protected def getOrCreateSparkSession(appName: String): SparkSession = sparkMock
  }

  trait TestSparkSessionProvider extends SparkSessionProvider {
    //noinspection SameParameterValue
    override protected def getOrCreateSparkSession(appName: String): SparkSession = spark
  }

  class MockApp extends SparkApplication[TestConfig] with DefaultKafka {
    var originalTransformations: Boolean = false

    override def name: String = "kafka"

    override def script(): Unit = {}

    override def config: TestConfig = new TestConfig {
      kafka = new KafkaSection {
        bootstrapServers = "test.com:9092,test2.com:9092"
      }
    }

    override private[kafka] def kafkaDatasetFrom(source: DataFrame) = {
      if (originalTransformations) {
        super.kafkaDatasetFrom(source)
      }
      else {
        kafkaDs
      }
    }
  }

  it should "should set proper configuration based on application config for read" in {
    val app = new MockApp with MockSparkSessionProvider

    app.readKafkaStream("test_topic")

    verify(sparkMock, times(1)).readStream
    verify(streamReader, times(1)).format("kafka")
    verify(streamReader, times(1)).option("kafka.bootstrap.servers", "test.com:9092,test2.com:9092")
    verify(streamReader, times(1)).option("subscribe", "test_topic")
    verify(streamReader, times(1)).option("startingOffsets", "latest")
    verify(streamReader, times(1)).option("failOnDataLoss", value = true)
    verify(streamReader, times(1)).load()
  }

  import spark.implicits._

  it should "return correct dataset of KafkaRecords" in {
    val app = new MockApp with TestSparkSessionProvider {
      originalTransformations = true
    }

    val timestamp = now

    val kafkaSourceDf = List(
      ("test_key", "test_value", timestamp, "test")
    ).toDF("key", "value", "timestamp", "test")

    val expected = List(
      KafkaRecord("test_key", "test_value", timestamp)
    ).toDS

    val actual = app.kafkaDatasetFrom(kafkaSourceDf)

    assertDatasetEquals(expected, actual)
  }

  it should "should set proper configuration based on application config for write" in {
    val app: MockApp with MockSparkSessionProvider = new MockApp with MockSparkSessionProvider {
      override private[kafka] def kafkaDatasetToWrite[A](
                                                          source: Dataset[A],
                                                          keyColumn: Option[String]) =
        kafkaWriteDs
    }

    app.writeStreamToKafka(kafkaDs, "test_topic", "checkpoint/test")

    verify(kafkaWriteDs, times(1)).writeStream
    verify(streamWriter, times(1)).format("kafka")
    verify(streamWriter, times(1)).option("kafka.bootstrap.servers", "test.com:9092,test2.com:9092")
    verify(streamWriter, times(1)).option("topic", "test_topic")
    verify(streamWriter, times(1)).option("checkpointLocation", "checkpoint/test")
    verify(streamWriter, times(1)).start()
    verify(streamingQuery, times(1)).awaitTermination()
  }

  it should "should set proper configuration based on application config for write batch" in {
    val app: MockApp with MockSparkSessionProvider = new MockApp with MockSparkSessionProvider {
      override private[kafka] def kafkaDatasetToWrite[A](
                                                          source: Dataset[A],
                                                          keyColumn: Option[String]) =
        kafkaWriteDs
    }

    app.writeToKafka(kafkaDs, "test_topic")

    verify(kafkaWriteDs, times(1)).writeStream
    verify(dfWriter, times(1)).format("kafka")
    verify(dfWriter, times(1)).option("kafka.bootstrap.servers", "test.com:9092,test2.com:9092")
    verify(dfWriter, times(1)).option("topic", "test_topic")
    verify(dfWriter, times(1)).save()
  }

  it should "serialize dataset records to json with key if it is specified" in {
    val ds = List(
      TestRecord(1, "test1"),
      TestRecord(2, "test2")
    ).toDS()

    val expected = List(
      KafkaWriteRecord("1", "{\"id\":1,\"record\":\"test1\"}"),
      KafkaWriteRecord("2", "{\"id\":2,\"record\":\"test2\"}")
    ).toDS()

    val app = new MockApp with TestSparkSessionProvider

    val actual = app.kafkaDatasetToWrite(ds, Some("id"))

    assertDatasetEquals(expected, actual)
  }

  it should "serialize dataset records to json with \"null\" as a key if it is not specified" in {
    val ds = List(
      TestRecord(1, "test1"),
      TestRecord(2, "test2")
    ).toDS()

    val expected = List(
      KafkaWriteRecord(null, "{\"id\":1,\"record\":\"test1\"}"),
      KafkaWriteRecord(null, "{\"id\":2,\"record\":\"test2\"}")
    ).toDS()

    val app = new MockApp with TestSparkSessionProvider

    val actual = app.kafkaDatasetToWrite(ds, None)

    assertDatasetEquals(expected, actual)
  }
}

case class TestRecord(id: Long, record: String)

 */