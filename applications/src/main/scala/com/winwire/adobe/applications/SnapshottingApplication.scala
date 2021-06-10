package com.winwire.adobe.applications
/**
  * Created by Naveen Gajja on 06/09/2021.
  */
import com.winwire.adobe.execution.deltalake.DeltaLake
import com.winwire.adobe.execution.kafka.{DefaultKafka, KafkaRecord, KafkaRecordParser}
import com.winwire.adobe.execution.spark.SparkApplication
import org.apache.spark.sql.Dataset

class SnapshottingApplication extends SparkApplication[SnapshottingConfig]
  with DefaultKafka
  with KafkaRecordParser
  with DeltaLake {

  override def name: String = config.jobName

  override def script(): Unit = {
    val sourceDs: Dataset[KafkaRecord] = readKafkaStream(config.sourceTopic)

    sourceDs.writeStream
      .foreachBatch((ds, _) => processBatch(ds))
      .option("checkpointLocation", config.checkpointLocation)
      .start()
      .awaitTermination()
  }

  protected def processBatch(ds: Dataset[KafkaRecord]): Unit =
    Snapshotting(this).saveSnapshot(ds, config.sinkTable)
}

object SnapshottingApp extends App {
  new SnapshottingApplication().main(args)
}
