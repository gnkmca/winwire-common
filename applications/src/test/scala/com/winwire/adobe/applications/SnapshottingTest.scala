package com.winwire.adobe.applications

import java.sql.Timestamp
import java.time.Instant

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.winwire.adobe.execution.deltalake.DeltaLake
import com.winwire.adobe.execution.kafka.{KafkaRecord, KafkaRecordParser}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchers._

class SnapshottingTest extends BaseTest with MockitoSugar with DatasetSuiteBase {
  behavior of classOf[Snapshotting].getName

  import spark.implicits._

  private val parser = mock[KafkaRecordParser]
  private val sink = mock[DeltaLake]

  it should "not write any data if input dataset is empty" in {
    val snapshotting = new Snapshotting(parser, sink)
    val records: List[KafkaRecord] = List()

    implicit val sp: SparkSession = spark
    snapshotting.saveSnapshot(records.toDS(), "test")

    when(parser.dataframeFromKafkaRecords(any[Dataset[KafkaRecord]])(any[SparkSession]))
      .thenReturn(records.toDS().toDF)

    verify(sink, never).writeToDelta(any[Dataset[KafkaRecord]], any[String], any[Option[String]])
  }

  it should "call json transformation and write if input dataset is not empty" in {
    val snapshotting = new Snapshotting(parser, sink)
    val records: List[KafkaRecord] = List(
      KafkaRecord("test_key", "test_value", Timestamp.from(Instant.now()))
    )

    when(parser.dataframeFromKafkaRecords(any[Dataset[KafkaRecord]])(any[SparkSession]))
      .thenReturn(records.toDS().toDF)

    implicit val sp: SparkSession = spark
    snapshotting.saveSnapshot(records.toDS(), "test")

    verify(parser, times(1)).dataframeFromKafkaRecords(any[Dataset[KafkaRecord]])(any[SparkSession])
    verify(sink, times(1)).writeToDelta(any[Dataset[KafkaRecord]], any[String], any[Option[String]])
  }
}
