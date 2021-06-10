package com.winwire.adobe.ingestion.etl.util

import com.winwire.adobe.execution.Application
import com.winwire.adobe.jdbc.JdbcClient
import com.winwire.adobe.ingestion.etl.config.MetastoreConfig
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}

object FlowStatus {
  val SUCCEEDED: String = "SUCCEEDED"
  val IN_PROGRESS: String = "IN_PROGRESS"
  val FAILED: String = "FAILED"
}

object MetricNames {
  val EXTRACTED_BYTES = "extracted_bytes"
  val EXTRACTED_ROWS = "extracted_rows"
  val EXTRACTED_DATE = "extracted_date"
}

object RulesTypes {
  val COUNT_DIFF = "count"
  val DATE_DIFF = "date_diff"
  val EXTRACTED_COUNT_DIFF = "extracted_count"
}

object SourceTypes {
  val SFTP = "sftp"
  val KAFKA = "kafka"
  val BOX = "box"
  val INTERNAL = "internal"
  val DATABRICKS = "databricks"
  val JDBC = "jdbc"
  val S3 = "s3"
}

object MetricConstants {
  val PROCESS_ID_COLUMN: String = "process_id"
}

case class Metric(name: String, value: Long, source: String = "")

trait MetastoreUtils {

  this: Application[_ <: MetastoreConfig] =>

  protected lazy val meta: JdbcClient = new JdbcClient(config.metastore.connectionDetails)

  def startProcess(flowName: String): String = {
    val result = meta.updateSQL(
      s"""
         |INSERT INTO ${config.metastore.etlProcessesTable} (flow_name, status, start_time)
         |VALUES ('$flowName', '${FlowStatus.IN_PROGRESS}', '$getCurrentDateTime')
         """.stripMargin, returnKey = true
    )
    result match {
      case Some(uniqueId) => uniqueId.toString
      case None =>
        logger.error(s"Failed to insert process for '$flowName' into metastore: table: ${config.metastore.etlProcessesTable}," +
          s" flowName: $flowName")
        null
    }
  }

  private def getCurrentDateTime: String = {
    new DateTime().toString("yyyy-MM-dd HH:mm:ss")
  }

  def updateProcess(flowName: String, newStatus: String, processId: String): Unit = {
    if (StringUtils.isNotBlank(processId)) {
      val updatedRows = meta.updateSQL(
        s"""|UPDATE ${config.metastore.etlProcessesTable}
            |SET status = '$newStatus', end_time = '$getCurrentDateTime'
            |WHERE flow_name = '$flowName' AND status = '${FlowStatus.IN_PROGRESS}'
            |AND ${MetricConstants.PROCESS_ID_COLUMN} = $processId
         """.stripMargin, returnKey = false
      )

      updatedRows match {
        case Some(0) =>
          logger.info(s"Status of flow_name = '$flowName' was already updated previously. " +
            s"Or process doesn't exist. Unable to set '$newStatus'")
        case Some(affectedRows) =>
          logger.info(s"Status of flow_name = '$flowName' was successfully changed to $newStatus " +
            s"affected rows: $affectedRows")
        case None =>
          logger.error(s"A problem occurred while updating '${config.metastore.etlProcessesTable}' table status. " +
            s"flow_name = '$flowName'  newStatus: '$newStatus'")
      }
    } else {
      logger.warn(s"Unable to update process. Process doesn't exist, processId='$processId''")
    }
  }

  def extractMetricResult(metricResult: Try[DataFrame]): Array[(String, Long)] = {
    val rawMetricEncoder: Encoder[(String, Long)] = Encoders.tuple(Encoders.STRING, Encoders.scalaLong)
    metricResult match {
      case Success(df) =>
        df.map(row => (row.getString(0), row.getLong(1)))(rawMetricEncoder).collect()
      case Failure(exception) =>
        logger.error(s"Unable to extract or calculate metric. Exception: $exception")
        Array.empty
    }
  }

  def getProcessedFiles(flowName: String): Array[String] = {
    meta.executeSQL[String](
      s"""|SELECT file_name
          |FROM ${config.metastore.processedFilesTable} f
          |JOIN ${config.metastore.etlProcessesTable} p
          |ON f.process_id = p.process_id
          |WHERE f.flow_name = '$flowName' AND p.status = '${FlowStatus.SUCCEEDED}'
          |UNION
          |SELECT file_name
          |FROM ${config.metastore.processedFilesTable} p
          |WHERE p.flow_name = '$flowName' AND p.process_id = 0
       """.stripMargin
    )(rs => rs.getString("file_name")).toArray
  }

  def getProcessedFilesAndVersions(flowName: String): Array[(String, Int)] = {
    meta.executeSQL[(String, Int)](
      s"""|SELECT file_name, version
          |FROM ${config.metastore.processedFilesTable} f
          |JOIN ${config.metastore.etlProcessesTable} p
          |ON f.process_id = p.process_id
          |WHERE f.flow_name = '$flowName' AND p.status = '${FlowStatus.SUCCEEDED}'
          |UNION
          |SELECT file_name, version
          |FROM ${config.metastore.processedFilesTable} p
          |WHERE p.flow_name = '$flowName' AND p.process_id = 0
       """.stripMargin
    )(rs => (rs.getString("file_name"), rs.getInt("version"))).toArray
  }

  def upsertProcessedFiles(processId: String, flowName: String, files: Seq[(String, Int)]): Unit = {
    for (file <- files) {
      val result = meta.updateSQL(
        s"""
           |MERGE INTO ${config.metastore.processedFilesTable} WITH (HOLDLOCK) AS target
           |USING
           |(SELECT '$flowName' AS flow_name,
           |'${file._1}' AS file_name) AS source
           |ON (target.flow_name = source.flow_name
           |  AND target.file_name = source.file_name)
           |WHEN MATCHED THEN
           |  UPDATE SET
           |    target.process_id  = $processId,
           |    target.version = ${file._2}
           |WHEN NOT MATCHED THEN
           |  INSERT (process_id, flow_name, file_name, version)
           |  VALUES ($processId, '$flowName', '${file._1}', ${file._2});
           |""".stripMargin, returnKey = true
      )
      result match {
        case Some(fileId) =>
          logger.info(s"'$file'[$fileId] file was persisted into metastore")
        case None =>
          throw new RuntimeException(s"Failed to upsert '$file' file into metastore")
      }
    }
  }

  def insertDownloadedFiles(processId: String, flowName: String, files: Seq[String], version: Int = 0): Unit = {
    for (fileName <- files) {
      val result = meta.updateSQL(
        s"""
           |INSERT INTO ${config.metastore.processedFilesTable} (process_id, flow_name, file_name, version)
           |VALUES ($processId, '$flowName', '$fileName', $version)
         """.stripMargin, returnKey = true
      )
      result match {
        case Some(fileId) =>
          logger.info(s"'$fileName'[$fileId] file was marked as started into metastore")
        case None =>
          throw new RuntimeException(s"Failed to insert '$fileName' file into metastore")
      }
    }
  }

  def deleteUnprocessedFiles(processId: String): Unit = {
    val affectedRows = meta.updateSQL(
      s"""
         |DELETE f FROM ${config.metastore.processedFilesTable} f
         |JOIN ${config.metastore.etlProcessesTable} p
         |ON f.process_id = p.process_id
         |WHERE f.${MetricConstants.PROCESS_ID_COLUMN} = $processId AND p.status <> '${FlowStatus.SUCCEEDED}'
       """.stripMargin, returnKey = false
    )

    affectedRows match {
      case None => logger.error(s"A problem occurred while deleting files from ${config.metastore.processedFilesTable} " +
        s"table. process_id = '$processId'")
      case Some(0) => logger.info(s"There are no files to delete.")
      case Some(affectedRows) =>
        if (affectedRows > 0) {
          logger.info(s"Files were successfully deleted from '${config.metastore.processedFilesTable}' table. " +
            s"process_id = '$processId' affectedRows: '$affectedRows'")
        }
    }
  }

  def insertMetric(processId: String, metric: Metric): Unit = {
    if (StringUtils.isNotBlank(processId)) {
      val affectedRows = meta.updateSQL(
        s"""
           |INSERT INTO ${config.metastore.etlMetricsTable} (process_id, updated_at, name, source, value)
           |VALUES ($processId, '$getCurrentDateTime', '${metric.name}', '${metric.source}', '${metric.value}')
         """.stripMargin, returnKey = false
      )

      affectedRows match {
        case None | Some(0) =>
          logger.error(s"A problem occurred while inserting metric name= '${metric.name}' into '${config.metastore.etlMetricsTable}' table. " +
            s"process_id = '$processId' value: '${metric.value}'")
        case Some(affectedRows) =>
          if (affectedRows > 0) {
            logger.info(s"Metric = '${metric.name}' was successfully inserted into '${config.metastore.etlMetricsTable}' table. " +
              s"process_id = '$processId' value: '${metric.value}'")
          }
      }
    } else {
      logger.warn(s"Unable to insert metric using processId='$processId''")
    }
  }
}