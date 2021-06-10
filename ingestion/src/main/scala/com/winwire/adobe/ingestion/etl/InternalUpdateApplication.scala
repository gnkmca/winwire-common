package com.winwire.adobe.ingestion.etl

import com.winwire.adobe.ingestion.etl.config.InternalApplicationConfig
import com.winwire.adobe.ingestion.etl.sql.TableConfig
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

class InternalUpdateApp[T <: TableConfig, C <: InternalApplicationConfig[T] : ClassTag] extends IngestionApplication[T, C] {

  override def script(): Unit = {
    val params = initEtlParams(config.table)
    run(config.table, params)
  }

  override protected def extractData(table: T, params: Map[String, String]): DataFrame = {
    logger.info("INTERNAL Source")
    executeSql(config.table.dataSql, params)
  }
}


object InternalUpdateApplication extends App {
  new InternalUpdateApp[TableConfig, InternalApplicationConfig[TableConfig]]().main(args)
}