package com.winwire.adobe.ingestion.etl.datawriter

import com.winwire.adobe.execution.spark.SparkApplication
import org.apache.spark.sql.DataFrame

trait DataFileWriter {

  this: SparkApplication[_] =>

  def writeRawData(df: DataFrame, config: DataFileWriterConfig, path: String, mode: String): Unit = {
    df.write
      .format(config.rawDataFormat)
      .mode(mode)
      .options(config.writeOptionsAsMap)
      .save(path)
  }
}