package com.winwire.adobe.ingestion.etl.lithium

import org.apache.spark.sql.types.StructType

abstract class AbstractTable(val name: String, val partitionCol: Option[String] = None) extends Serializable

abstract class AbstractTypedTable(val schema: YamlSchema) extends AbstractTable(schema.tableName, None) {
  def structure(): StructType = {
    var struct = new StructType()
    for (column <- this.schema.columns) {
      struct = struct.add(column.name.replace(".", "_"), column.dataType)
    }
    struct
  }
}

class NamedTable(name: String) extends AbstractTable(name: String, None)
