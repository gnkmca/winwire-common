package com.winwire.adobe.ingestion.etl.datareader

import com.winwire.adobe.ingestion.etl.datareader.xml.XSDToSchema
import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType

object SchemaUtils {

  val DDL = "ddl"
  val AVSC = "avsc"
  val XSD = "xsd"


  def schemaFor(schema: String, format: String): StructType = {
    format match {
      case DDL => StructType.fromDDL(schema)
      case AVSC =>
        val avroSchema = new Schema.Parser().parse(schema)
        SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
      case XSD => XSDToSchema.xsdToSparkSchema(schema)
      case _ => throw new IllegalArgumentException(s"Unsupported schema format [$format]")
    }
  }
}
