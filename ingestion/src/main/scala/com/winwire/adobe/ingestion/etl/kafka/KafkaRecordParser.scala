/*
package com.winwire.adobe.ingestion.etl.kafka

import com.winwire.adobe.execution.kafka.KafkaRecord
import com.winwire.adobe.execution.spark.SparkApplication
import com.winwire.adobe.ingestion.etl.config.KafkaTableConfig
import com.winwire.adobe.ingestion.etl.datareader.SchemaUtils
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{col, explode, from_json, lit, udf}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.JavaConverters._

trait KafkaRecordParser {

  this: SparkApplication[_] =>

  import spark.implicits._

  private val fromSchemaRegistryAvroRecord = udf((bytes: Array[Byte], avroStringSchema: String) => {
    val avroSchema = new Schema.Parser().parse(avroStringSchema)
    val inputStream = new SeekableByteArrayInput(bytes)
    val datumReader = new GenericDatumReader[GenericRecord](avroSchema)
    val dataFileReader = new DataFileReader[GenericRecord](inputStream, datumReader)

    val values = dataFileReader.iterator().asScala.toArray
    dataFileReader.close()

    values.map(v => v.toString)
  })

  def parseMessages[T <: KafkaTableConfig](input: Dataset[KafkaRecord], table: T): DataFrame = {

    val schema = SchemaUtils.schemaFor(table.inputDataSchema, table.inputDataSchemaFormat)

    val jsonDf = table.rawDataFormat match {
      case "schema_registry_avro" =>

        logger.info(s"Parsing kafka schema_registry_avro messages")
        val readOptions = table.readOptionsAsMap
        if (readOptions.contains("schemaRegistryUrl")) {
          logger.info("Using schema registry")
          throw new UnsupportedOperationException("Parsing kafka schema_registry_avro using schemaRegistryUrl not implemented")

        } else {
          logger.info("Using raw schema")
          val avroSchema = SchemaConverters.toAvroType(schema).toString
          input
            .select(fromSchemaRegistryAvroRecord(col("value"), lit(avroSchema)).alias("json"))
            .withColumn("json", explode(col("json")))
        }

      case "json" =>
        logger.info(s"Parsing kafka json messages")
        import spark.implicits._
        input.select($"value".cast("string").as("json"))
      case _ =>
        throw new IllegalArgumentException(s"Unsupported rawDataFormat [${table.rawDataFormat}]")
    }

    jsonDf
      .select(from_json($"json", schema).as("json"))
      .select($"json.*")
  }
}


 */