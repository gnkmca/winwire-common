package com.winwire.adobe.ingestion.etl.datareader

import com.winwire.adobe.execution.spark.SparkApplication
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}


import java.nio.file.Paths

trait DataFileReader {

  this: SparkApplication[_] =>

  def readRawData(config: DataFileReaderConfig, schema: StructType, paths: String*): DataFrame = {
    val reader = initializeReader(schema)
    val options = config.readOptions.map(kv => (kv.key, kv.value)).toMap

    config.rawDataFormat match {
      case "csv" | "text" | "parquet" | "delta" | "json" | "avro" => read(reader, config.rawDataFormat, paths, options)
      // TODO: increase version of package to use native XSDToSchema
      case "xml" => readXml(reader, paths.head, options)
      case "orc" => readOrc(reader, paths, options)
      case _ => throw new IllegalArgumentException(s"Unsupported file format: ${config.rawDataFormat}")
    }
  }


  private def readOrc(reader: DataFrameReader, paths: Seq[String], options: Map[String, String]): DataFrame = {
    val finalPaths = if (options.contains("withHiveUnionSubdirs") && options("withHiveUnionSubdirs").equalsIgnoreCase("true")
    ) {
      // TODO: fix *
      paths.map(Paths.get(_, "HIVE_UNION_SUBDIR*").toString)
    } else {
      paths
    }
    reader
      .format("orc")
      .options(options)
      .load(finalPaths: _*)
  }


  private def readXml(reader: DataFrameReader, path: String, options: Map[String, String]): DataFrame = {
    reader
      .format("com.databricks.spark.xml")
      .options(options)
      .load(path)
  }

  private def initializeReader(schema: StructType): DataFrameReader = {
    val reader = spark.read
    if (schema != null) {
      reader.schema(schema)
    }
    reader
  }

  private def read(reader: DataFrameReader, format: String, paths: Seq[String], options: Map[String, String]): DataFrame = {
    reader
      .format(format)
      .options(options)
      .load(paths: _*)
  }

}