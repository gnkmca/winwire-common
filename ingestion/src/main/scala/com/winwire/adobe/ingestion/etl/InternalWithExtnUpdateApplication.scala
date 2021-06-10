package com.winwire.adobe.ingestion.etl

import com.winwire.adobe.ingestion.etl.config.InternalWithExtnApplicationConfig
import com.winwire.adobe.ingestion.etl.sql.TableConfig
import com.winwire.adobe.ingestion.etl.util.ConfigUtils.updateTableConfig
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class InternalWithExtnUpdateApp[T <: TableConfig, C <: InternalWithExtnApplicationConfig[T] : ClassTag] extends InternalUpdateApp[T, C] {


  override protected def beforeEtl(table: T, params: Map[String, String]): Unit = {
    updateTableConfig(config.extnTable, config)

    if (StringUtils.isNotBlank(config.extnTable.createSql)) {
      createTable(config.extnTable)
    }

    super.beforeEtl(table, params)
  }

  override protected def transformAndLoad(extracted: DataFrame, table: T, params: Map[String, String]): Unit = {
    val version = getLastDeltaVersion(table)
    logger.info(s"Current delta version of normal table: $version")
    Try({

      logger.info(s"Load table ${table.name}")
      super.transformAndLoad(extracted, table, params)

      logger.info(s"Load extn table ${config.extnTable.name}")
      super.transformAndLoad(extracted, config.extnTable, initTableParams(config.extnTable))

    }) match {
      case Success(_) =>
      case Failure(exception) =>
        logger.error(s"Exception occurred during writing: $exception")
        rollbackTable(table, version)
        throw exception
    }
  }
}


object InternalWithExtnUpdateApplication extends App {
  new InternalWithExtnUpdateApp[TableConfig, InternalWithExtnApplicationConfig[TableConfig]]().main(args)
}
