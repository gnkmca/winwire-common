package com.winwire.adobe.ingestion.etl

import com.winwire.adobe.execution.spark.SparkApplication
import com.winwire.adobe.ingestion.etl.config.AppConfig
import com.winwire.adobe.ingestion.etl.sql.{IngestionSql, TableConfig, TableCreator}
import com.winwire.adobe.ingestion.etl.util.ConfigUtils.updateTableConfig
import com.winwire.adobe.ingestion.etl.util.Params.insertParams
import com.winwire.adobe.ingestion.etl.util._
import com.winwire.adobe.ingestion.etl.util.MetastoreUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

case class EtlStatus(flowStatus: String, exception: Throwable = null)

abstract class IngestionApplication[T <: TableConfig, C <: AppConfig : ClassTag] extends SparkApplication[C]
  with TableCreator
  with IngestionSql
  with MetastoreUtils
  with UDFUtils {

  import IngestionApplication._

  override def name: String = config.jobName

  override def configure(spark: SparkSession): Unit = {
    config.app.sparkConf
      .foreach(kv => spark.conf.set(kv.key, kv.value))
    config.app.hadoopConf
      .foreach(kv => spark.sparkContext.hadoopConfiguration.set(kv.key, kv.value))
  }

  override protected def getOrCreateSparkSession(appName: String): SparkSession = {
    val spark = super.getOrCreateSparkSession(appName).newSession()
    getUDFs.foreach(e => {
      logger.info(s"Register ${e._1}")
      spark.udf.register(e._1, e._2)
    })
    spark
  }

  protected def initTableParams(table: T): Map[String, String] = {
    val params = config.app.optionsAsMap ++ Map[String, String](
      EXTRACTED_VIEW_KEY -> s"${table.name}_extracted",
      OUTPUT_VIEW_KEY -> s"${table.name}_output",
      TARGET_TABLE_KEY -> table.name,
      TARGET_DB_KEY -> table.db,
      FLOW_NAME_KEY ->  flowNameOf(table),
      UPDATE_DATE_COLUMN_KEY -> table.updateDateColumn,
      UPDATE_DATE_COLUMN_FORMAT_KEY -> config.app.optionsAsMap.getOrElse(UPDATE_DATE_COLUMN_FORMAT_KEY, DATE_FORMAT)
    )

    if (StringUtils.isNotBlank(table.updateDateColumn)) {
      val lastUpdateDate = getMaxUpdateDateColumnValue(table) match {
        case Some(value) => value
        case None => DateTimeFormatter
          .ofPattern(params(UPDATE_DATE_COLUMN_FORMAT_KEY))
          .format(LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0))
      }
      params ++ Map[String, String](LAST_UPDATE_DATE_KEY -> lastUpdateDate)
    } else {
      params
    }
  }
  protected def initEtlParams(table: T): Map[String, String] = {
    val flowName = flowNameOf(table)
    val processId = startProcess(flowName)
    logger.info(s"Pipeline process_id is '$processId'")

    initTableParams(table) ++ Map[String, String](
      PROCESS_ID_KEY -> processId
    )
  }

  protected def flowNameOf(table: T): String = if (config.app.isHistorical) s"${table.name}_historical" else table.name


  protected def run(table: T, params: Map[String, String] = Map.empty): Unit = {
    val etlStatus = Try {
      beforeEtl(table, params)

      val extractedData = extractData(table, params)

      if (hasNewData(table, extractedData, params)) {
        extractedData.createOrReplaceTempView(params(EXTRACTED_VIEW_KEY))
        transformAndLoad(extractedData, table, params)
      } else {
        logger.info(s"No new data for ${table.name}")
      }

    } match {
      case Success(_) => EtlStatus(FlowStatus.SUCCEEDED)
      case Failure(exception) => EtlStatus(FlowStatus.FAILED, exception)
    }
    afterEtl(table, etlStatus, params)

    identifyEtlStatus(etlStatus)
  }

  protected def hasNewData(table: T, extractedData: DataFrame, params: Map[String, String]): Boolean = {
    true
  }

  protected def beforeEtl(table: T, params: Map[String, String]): Unit = {
    updateTableConfig(table, config)

    if (StringUtils.isNotBlank(table.createSql)) {
      createTable(table)
    }
  }

  protected def afterEtl(table: T, etlStatus: EtlStatus, params: Map[String, String]): Unit = {
    val processId = params(PROCESS_ID_KEY)
    updateProcess(params(FLOW_NAME_KEY), etlStatus.flowStatus, processId)
    val additionalMetrics = gatherAdditionalMetrics(table, params)
    saveMetrics(processId, additionalMetrics: _*)
  }

  protected def gatherAdditionalMetrics(table: T, params: Map[String, String]): Array[Metric] = {
    var metrics: Array[Metric] = Array.empty

    if (spark.catalog.tableExists(table.db, table.name)) {
      if (StringUtils.isNotBlank(table.outputMetricSql)) {
        val query = insertParams(table.outputMetricSql, params)
        val values = extractMetricResult(Try(executeSql(query)))
        logger.info(s"outputMetricSql values: ${values.mkString("Array(", ", ", ")")}; source: ${SourceTypes.DATABRICKS}")
        metrics ++= values.map(outputMetric => Metric(outputMetric._1, outputMetric._2, SourceTypes.DATABRICKS))
      }
    }

    metrics
  }

  protected def saveMetrics(processId: String, metrics: Metric*): Unit = {
    metrics
      .foreach(metric => insertMetric(processId, metric))
  }

  protected def identifyEtlStatus(etlStatus: EtlStatus): Unit = if (etlStatus.flowStatus == FlowStatus.FAILED) throw etlStatus.exception

  protected def extractData(table: T, params: Map[String, String] = Map.empty): DataFrame = {
    throw new UnsupportedOperationException("extractData() method is not implemented.")
  }

  protected def transformAndLoad(extracted: DataFrame, table: T, params: Map[String, String] = Map.empty): Unit = {
    extracted.createOrReplaceTempView(params(EXTRACTED_VIEW_KEY))
    val tableAfterTransformation = if (table.preprocessSql.nonEmpty) {
      logger.info("Start preprocess step")
      executeSql(table.preprocessSql, params)
    } else {
      extracted
    }

    val distinctRows = if (StringUtils.isNoneBlank(table.primaryKeys, table.updateDateColumn)) {
      logger.info("Start dropDuplicates step")
      dropDuplicates(tableAfterTransformation, table)
    } else {
      tableAfterTransformation
    }

    logger.info("Start write step")
    distinctRows.createOrReplaceTempView(params(OUTPUT_VIEW_KEY))
    saveOutputData(distinctRows, table, params)
  }

  protected def saveOutputData(input: DataFrame, table: T, params: Map[String, String] = Map.empty): Unit = {
    writeTable(input, table, params)
  }

  protected def getExtractedRowMetric(df: DataFrame): Metric = {
    val extractedRows = df.count()
    logger.info(s"Extracted rows are '$extractedRows'")
    Metric(MetricNames.EXTRACTED_ROWS, extractedRows)
  }

}

object IngestionApplication {
  val EXTRACTED_VIEW_KEY = "extractedData"
  val OUTPUT_VIEW_KEY = "outputData"
  val TARGET_DB_KEY = "targetDb"
  val TARGET_TABLE_KEY = "targetTable"
  val FLOW_NAME_KEY = "flowName"
  val PROCESS_ID_KEY = "processId"
  val UPDATE_DATE_COLUMN_FORMAT_KEY = "updateDateColumnFormat"
  val UPDATE_DATE_COLUMN_KEY = "updateDateColumn"
  val LAST_UPDATE_DATE_KEY = "lastUpdateDate"
  val DATE_FORMAT = "yyyy-MM-dd"
}