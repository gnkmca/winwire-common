package com.winwire.adobe.execution.kafka

import org.apache.spark.sql.functions.{from_json, schema_of_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

trait KafkaRecordParser {

  def datasetFromKafkaRecords[A <: Product : TypeTag](input: Dataset[KafkaRecord])
                                                     (implicit spark: SparkSession): Dataset[A] = {
    import spark.implicits._

    val encoder: Encoder[A] = Encoders.product[A]
    val schema: StructType = encoder.schema
    val checker = SchemaChecker[A](schema)

    flattenKafkaRecords(input, Left(schema))
      .filter(row => checker.conform(row))
      .as[A]
  }

  private def flattenKafkaRecords(input: Dataset[KafkaRecord],
                                  valueSchema: Either[StructType, Column])
                                 (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val column = $"value"

    val fromJson = valueSchema match {
      case Left(s) => from_json(column, s)
      case Right(c) => from_json(column, c)
    }

    input
      .select(fromJson.alias("json"), $"timestamp")
      .select("json.*", "timestamp")
  }

  def dataframeFromKafkaRecords(input: Dataset[KafkaRecord])
                               (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val json = input.map(_.value).first()
    val schema = schema_of_json(json)

    flattenKafkaRecords(input, Right(schema))
  }
}
