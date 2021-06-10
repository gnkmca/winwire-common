package com.winwire.adobe.ingestion.etl.sql

import com.winwire.adobe.execution.deltalake.DeltaLake
import com.winwire.adobe.execution.spark.{SparkApplication, SparkSessionProvider}
import com.winwire.adobe.ingestion.etl.BaseTest
import org.apache.spark.api.java.function.VoidFunction2
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, _}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach

class IngestionSqlTest extends BaseTest
  with MockitoSugar
  with BeforeAndAfterEach {

  behavior of classOf[IngestionSqlTest].getSimpleName

  private var sparkMock: SparkSession = _
  private var streamWriter: DataStreamWriter[Row] = _
  private var df: DataFrame = _
  private var streamingQuery: StreamingQuery = _
  private var dfWriter: DataFrameWriter[Row] = _

  override def beforeEach(): Unit = {
    sparkMock = mock[SparkSession]
    streamWriter = mock[DataStreamWriter[Row]]
    df = mock[DataFrame]
    streamingQuery = mock[StreamingQuery]
    dfWriter = mock[DataFrameWriter[Row]]

    when(sparkMock.sql(any[String])).thenReturn(df)

    when(df.write).thenReturn(dfWriter)
    when(dfWriter.format(any[String])).thenReturn(dfWriter)
    when(dfWriter.mode(any[SaveMode])).thenReturn(dfWriter)
    when(dfWriter.option(any[String], any[String])).thenReturn(dfWriter)
    when(dfWriter.partitionBy(any[String])).thenReturn(dfWriter)
    doNothing.when(dfWriter).save(any[String])

    when(df.writeStream).thenReturn(streamWriter)
    when(streamWriter.queryName(any[String])).thenReturn(streamWriter)
    when(streamWriter.trigger(any[Trigger])).thenReturn(streamWriter)
    when(streamWriter.format(any[String])).thenReturn(streamWriter)
    when(streamWriter.option(any[String], any[String])).thenReturn(streamWriter)
    when(streamWriter.outputMode(any[String])).thenReturn(streamWriter)
    when(streamWriter.foreachBatch(any[VoidFunction2[Dataset[Row], java.lang.Long]])).thenReturn(streamWriter)
    when(streamWriter.start(any[String])).thenReturn(streamingQuery)
  }

  trait MockSparkSessionProvider extends SparkSessionProvider {
    override protected def getOrCreateSparkSession(appName: String): SparkSession = sparkMock
  }

  class TestConfig

  class MockApp extends SparkApplication[TestConfig] with DeltaLake with IngestionSql {
    var originalTransformations: Boolean = false

    override def name: String = "sql trait"

    override def script(): Unit = {}

    override def config: TestConfig = new TestConfig

    override implicit lazy val spark: SparkSession = sparkMock
  }

  it should "write overwrite delta correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      saveMode = "overwrite"
      format = "delta"
      dbfsPath = "dbfsPath"
    }
    app.writeTable(df, tableConfig)

    verify(df, times(1)).write
    verify(dfWriter, times(1)).format("delta")
    verify(dfWriter, times(1)).mode(SaveMode.Overwrite)
    verify(dfWriter, times(1)).save("dbfsPath")
  }

  it should "write overwrite partitioned delta correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      partition = "partition"
      saveMode = "overwrite"
      format = "delta"
      dbfsPath = "dbfsPath"
    }
    app.writeTable(df, tableConfig)

    verify(df, times(1)).write
    verify(dfWriter, times(1)).format("delta")
    verify(dfWriter, times(1)).mode(SaveMode.Overwrite)
    verify(dfWriter, times(1)).partitionBy("partition")
    verify(dfWriter, times(1)).save("dbfsPath")
  }

  it should "write append delta correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      saveMode = "append"
      format = "delta"
      dbfsPath = "dbfsPath"
    }
    app.writeTable(df, tableConfig)

    verify(df, times(1)).write
    verify(dfWriter, times(1)).format("delta")
    verify(dfWriter, times(1)).mode(SaveMode.Append)
    verify(dfWriter, times(1)).save("dbfsPath")
  }

  it should "write append partitioned delta correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      partition = "partition"
      saveMode = "append"
      format = "delta"
      dbfsPath = "dbfsPath"
    }
    app.writeTable(df, tableConfig)

    verify(df, times(1)).write
    verify(dfWriter, times(1)).format("delta")
    verify(dfWriter, times(1)).mode(SaveMode.Append)
    verify(dfWriter, times(1)).partitionBy("partition")
    verify(dfWriter, times(1)).save("dbfsPath")
  }

  it should "write overwrite orc correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      saveMode = "overwrite"
      format = "orc"
      dbfsPath = "dbfsPath"
    }
    app.writeTable(df, tableConfig)

    verify(df, times(1)).write
    verify(dfWriter, times(1)).format("orc")
    verify(dfWriter, times(1)).mode(SaveMode.Overwrite)
    verify(dfWriter, times(1)).save("dbfsPath")
  }

  it should "write overwrite partitioned orc correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      partition = "partition"
      saveMode = "overwrite"
      format = "orc"
      dbfsPath = "dbfsPath"
    }
    app.writeTable(df, tableConfig)

    verify(df, times(1)).write
    verify(dfWriter, times(1)).format("orc")
    verify(dfWriter, times(1)).mode(SaveMode.Overwrite)
    verify(dfWriter, times(1)).partitionBy("partition")
    verify(dfWriter, times(1)).save("dbfsPath")
  }

  it should "write append orc correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      saveMode = "append"
      format = "orc"
      dbfsPath = "dbfsPath"
    }
    app.writeTable(df, tableConfig)

    verify(df, times(1)).write
    verify(dfWriter, times(1)).format("orc")
    verify(dfWriter, times(1)).mode(SaveMode.Append)
    verify(dfWriter, times(1)).save("dbfsPath")
  }

  it should "write append partitioned orc correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      partition = "partition"
      saveMode = "append"
      format = "orc"
      dbfsPath = "dbfsPath"
    }
    app.writeTable(df, tableConfig)

    verify(df, times(1)).write
    verify(dfWriter, times(1)).format("orc")
    verify(dfWriter, times(1)).mode(SaveMode.Append)
    verify(dfWriter, times(1)).partitionBy("partition")
    verify(dfWriter, times(1)).save("dbfsPath")
  }

  it should "not write invalid output mode" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      saveMode = "unknown"
    }

    val thrown = intercept[Exception] {
      app.writeTable(df, tableConfig)
    }
    thrown.getClass shouldBe classOf[UnsupportedOperationException]
  }

  it should "update delta data by query" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      saveMode = "update"
      format = "delta"
      dbfsPath = "dbfsPath"
      foreachBatchSql = "foreachBatchSql"
    }
    app.writeTable(df, tableConfig)
    verify(sparkMock, times(1)).sql("foreachBatchSql")
  }

  it should "write stream df correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      saveMode = "append"
      name = "tableName"
      format = "delta"
      dbfsPath = "dbfsPath"
    }
    app.writeStream(df, tableConfig, "checkpointLocation")

    verify(df, times(1)).writeStream
    verify(streamWriter, times(1)).queryName("tableName")
    verify(streamWriter, times(1)).trigger(Trigger.Once())
    verify(streamWriter, times(1)).format("delta")
    verify(streamWriter, times(1)).option("checkpointLocation", "checkpointLocation")
    verify(streamWriter, times(1)).outputMode("append")
    verify(streamWriter, times(1)).option("path", "dbfsPath")
    verify(streamWriter, times(1)).start()
  }

  it should "write stream df (with foreach batch sql) correctly" in {
    val app = new MockApp with MockSparkSessionProvider
    val tableConfig = new TableConfig {
      name = "tableName"
      format = "delta"
      foreachBatchSql = "foreachBatchSql"
    }
    app.writeStream(df, tableConfig, "checkpointLocation")

    verify(df, times(1)).writeStream
    verify(streamWriter, times(1)).queryName("tableName")
    verify(streamWriter, times(1)).trigger(Trigger.Once())
    verify(streamWriter, times(1)).format("delta")
    verify(streamWriter, times(1)).option("checkpointLocation", "checkpointLocation")
    verify(streamWriter, times(1)).outputMode("update")
    //    verify(streamWriter, times(1)).foreachBatch((_: DataFrame, _: Long) => {})
    verify(streamWriter, times(1)).start()
  }
}
