package com.winwire.adobe.applications
/**
  * Created by Naveen Gajja on 06/09/2021.
  */
import com.winwire.adobe.execution.deltalake.DeltaLake
import com.winwire.adobe.execution.kafka.{KafkaRecord, KafkaRecordParser}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

private[applications] class Snapshotting(parser: KafkaRecordParser, sink: DeltaLake) {
  private[this] val PARTITION_COLUMN = "date"

  def saveSnapshot(ds: Dataset[KafkaRecord], table: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val datasetEmpty = ds.head(1).isEmpty

    if (!datasetEmpty) {
      val outputDf = parser.dataframeFromKafkaRecords(ds)
        .withColumn(PARTITION_COLUMN, to_date($"timestamp", "yyyy-MM-dd"))

      sink.writeToDelta(outputDf, table, Some(PARTITION_COLUMN))
    }
  }
}

private[applications] object Snapshotting {
  def apply(app: SnapshottingApplication): Snapshotting = new Snapshotting(app, app)
}