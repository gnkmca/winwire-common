package com.winwire.adobe.ingestion.etl

import com.winwire.adobe.execution.jdbc.DefaultJdbc
import com.winwire.adobe.ingestion.etl.IngestionApplication.PROCESS_ID_KEY
import com.winwire.adobe.jdbc.{ConnectionDetails, JdbcClient}
import com.winwire.adobe.ingestion.etl.config.{JdbcApplicationConfig, JdbcTableConfig}
import com.winwire.adobe.ingestion.etl.util.Params.insertParams
import com.winwire.adobe.ingestion.etl.util.{FlowStatus, Metric, MetricConstants, SourceTypes}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag
import scala.util.Try

class JdbcUpdateApp[T <: JdbcTableConfig, C <: JdbcApplicationConfig[T] : ClassTag] extends IngestionApplication[T, C]
  with DefaultJdbc {

  override def script(): Unit = {
    val params = initEtlParams(config.table)
    run(config.table, params)
  }

  override  def updateProcess(flowName: String, newStatus: String, processId: String): Unit = {
   }




  override protected def extractData(table: T, params: Map[String, String]): DataFrame = {
    if (!table.useNativeJdbc) {
      readSparkJdbc(table, params)
    } else {
      readNativeJdbc(table, params)
    }
  }

  override protected def initEtlParams(table: T): Map[String, String] = {
    val flowName = flowNameOf(table)
    val processId = startProcess(flowName)
    logger.info(s"Pipeline process_id is '$processId'")

    initTableParams(table) ++ Map[String, String](
     // PROCESS_ID_KEY -> java.util.UUID.randomUUID().toString
      PROCESS_ID_KEY -> processId
    )
  }

  protected def readSparkJdbc(table: T, params: Map[String, String]): DataFrame = {
    logger.info("JDBC Source")
    val query = insertParams(table.dataSql, params)
    logger.info(query)

    readFromJdbc(query)
  }

  protected def readNativeJdbc(table: T, params: Map[String, String]): DataFrame = {
    logger.info("JDBC[native] Source")

    val jdbcClient = new JdbcClient(ConnectionDetails("default", Map(
      "default.url" -> config.jdbc.url,
      "default.driver" -> config.jdbc.driver,
      "default.username" -> config.jdbc.user,
      "default.password" -> config.jdbc.password
    )))

    val query = insertParams(table.dataSql, params)
    logger.info(s"Execute sql: $query")

    val data = jdbcClient.executeSQL(query)(rs => {
      val resultSetRecord = Range(0, rs.getMetaData.getColumnCount).map(i => rs.getString(i + 1))
      Row(resultSetRecord: _*)
    })

    val schema = StructType.fromDDL(table.inputDataSchema)
    if (data.nonEmpty) {
      val rdd = spark.sparkContext.parallelize(data)
      spark.createDataFrame(rdd, schema)
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }

  }


  override protected def gatherAdditionalMetrics(table: T, params: Map[String, String]): Array[Metric] = {
    var metrics: Array[Metric] = super.gatherAdditionalMetrics(table, params)

    if (StringUtils.isNotBlank(config.table.inputMetricSql)) {
      val query = insertParams(config.table.inputMetricSql, params)
      logger.info(s"inputMetricSql is available. query: $query")
      val values = extractMetricResult(Try(readFromJdbc(query)))
      logger.info(s"inputMetricsSql values: $values; source: ${SourceTypes.JDBC}")
      metrics ++= values.map(inputMetric => Metric(inputMetric._1, inputMetric._2, SourceTypes.JDBC))
    }
    metrics
  }
}


object JdbcUpdateApplication extends App {
  new JdbcUpdateApp[JdbcTableConfig, JdbcApplicationConfig[JdbcTableConfig]]().main(args)
}