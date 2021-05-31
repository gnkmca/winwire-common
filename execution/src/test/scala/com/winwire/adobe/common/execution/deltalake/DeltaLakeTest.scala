/*
package com.winwire.adobe.common.execution.deltalake

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.winwire.adobe.common.execution.BaseTest
import com.winwire.adobe.common.execution.kafka.KafkaRecord
import com.winwire.adobe.common.execution.spark.{SparkApplication, SparkSessionProvider}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql._
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach

class DeltaLakeTest extends BaseTest
  with MockitoSugar
  with BeforeAndAfterEach
  with DatasetSuiteBase {

  private var sparkMock: SparkSession = _
  private var streamWriter: DataStreamWriter[Row] = _
  private var kafkaDs: DataFrame = _
  private var streamingQuery: StreamingQuery = _
  private var dfReader: DataFrameReader = _
  private var dfWriter: DataFrameWriter[Row] = _

  override def beforeEach(): Unit = {
    sparkMock = mock[SparkSession]
    streamWriter = mock[DataStreamWriter[Row]]
    kafkaDs = mock[DataFrame]
    streamingQuery = mock[StreamingQuery]
    dfReader = mock[DataFrameReader]
    dfWriter = mock[DataFrameWriter[Row]]

    when(streamWriter.format(any[String])).thenReturn(streamWriter)
    when(streamWriter.option(any[String], any[String])).thenReturn(streamWriter)
    when(streamWriter.outputMode(any[String])).thenReturn(streamWriter)
    when(streamWriter.start(any[String])).thenReturn(streamingQuery)
    when(kafkaDs.writeStream).thenReturn(streamWriter)
    when(sparkMock.read).thenReturn(dfReader)
    when(kafkaDs.write).thenReturn(dfWriter)
    when(dfReader.format(any[String])).thenReturn(dfReader)
    when(dfReader.load(any[String])).thenReturn(kafkaDs)
    when(dfWriter.format(any[String])).thenReturn(dfWriter)
    when(dfWriter.mode(any[SaveMode])).thenReturn(dfWriter)
    when(dfWriter.option(any[String], any[String])).thenReturn(dfWriter)

  }

  class TestConfig

  trait TestSparkSessionProvider extends SparkSessionProvider {
    //noinspection SameParameterValue
    override protected def getOrCreateSparkSession(appName: String): SparkSession = spark
  }

  trait MockSparkSessionProvider extends SparkSessionProvider {
    //noinspection SameParameterValue
    override protected def getOrCreateSparkSession(appName: String): SparkSession = sparkMock
  }

  class MockApp extends SparkApplication[TestConfig] with DeltaLake {
    var originalTransformations: Boolean = false

    override def name: String = "delta lake"

    override def script(): Unit = {}

    override def config: TestConfig = new TestConfig
  }

  "DeltaLake" should "should set proper configuration based on parameters for write" in {
    val app = new MockApp with TestSparkSessionProvider

    app.writeStreamToDelta(kafkaDs, "test_topic", "checkpoint/test")

    verify(kafkaDs, times(1)).writeStream
    verify(streamWriter, times(1)).format("delta")
    verify(streamWriter, times(1)).outputMode("append")
    verify(streamWriter, times(1)).option("checkpointLocation", "checkpoint/test")
    verify(streamWriter, times(1)).start("test_topic")
    verify(streamingQuery, times(1)).awaitTermination()
  }

  "DeltaLake" should "should set proper configuration to read data" in {
    val app = new MockApp with MockSparkSessionProvider
    implicit val ss: SparkSession = app.spark

    app.readFromDelta[KafkaRecord]("test/path")

    verify(sparkMock, times(1)).read
    verify(dfReader, times(1)).format("delta")
    verify(dfReader, times(1)).load("test/path")
  }

  "DeltaLake" should "should set proper configuration to write data with Append mode" in {
    val app = new MockApp with MockSparkSessionProvider

    app.writeToDelta(kafkaDs, "test/path")

    verify(kafkaDs, times(1)).write
    verify(dfWriter, times(1)).format("delta")
    verify(dfWriter, times(1)).mode(SaveMode.Append)
    verify(dfWriter, times(1)).option("mergeSchema", "true")
    verify(dfWriter, times(1)).save("test/path")
  }

  "DeltaLake" should "should set proper configuration to write data" in {
    val app = new MockApp with MockSparkSessionProvider

    app.writeToDelta(kafkaDs, "test/path", SaveMode.Overwrite, None)

    verify(kafkaDs, times(1)).write
    verify(dfWriter, times(1)).format("delta")
    verify(dfWriter, times(1)).mode(SaveMode.Overwrite)
    verify(dfWriter, times(1)).option("mergeSchema", "true")
    verify(dfWriter, times(1)).save("test/path")
  }
}


 */