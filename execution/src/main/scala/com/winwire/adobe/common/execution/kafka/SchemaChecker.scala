package com.winwire.adobe.common.execution.kafka

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

import scala.util.Try
import scala.reflect.runtime.universe.{TypeTag, typeTag}

case class SchemaChecker[A <: Product : TypeTag](schema: StructType) extends LazyLogging {

  private val className: String = {
    val mirror = ScalaReflection.mirror
    val tpe = typeTag[A].in(mirror).tpe
    val cls = mirror.runtimeClass(tpe)
    cls.getCanonicalName
  }

  def conform(row: Row): Boolean = {

    def conform(row: Row, schema: StructType): Boolean = {
      schema.forall(field => {
        val isConform: Boolean = field match {
          case StructField(name, dataType, nullable, _) =>
            Try(row.fieldIndex(name))
              .map(index => {
                val isNull: Boolean = row.isNullAt(index)
                if (!nullable && isNull) {
                  false
                } else {
                  if (!isNull)
                    dataType match {
                      case s: StructType => conform(row.getStruct(index), s)
                      case _: BooleanType => Try(row.getBoolean(index)).isSuccess
                      case _: StringType => Try(row.getString(index)).isSuccess
                      case _: DateType => Try(row.getDate(index)).isSuccess
                      case _: TimestampType => Try(row.getTimestamp(index)).isSuccess
                      case _: DoubleType => Try(row.getDouble(index)).isSuccess
                      case _: FloatType => Try(row.getFloat(index)).isSuccess
                      case _: IntegerType => Try(row.getInt(index)).isSuccess
                      case _: LongType => Try(row.getLong(index)).isSuccess
                      case _: ByteType => Try(row.getByte(index)).isSuccess
                      case _: ShortType => Try(row.getShort(index)).isSuccess
                    }
                  else true
                }
              })
              .getOrElse(false)
        }
        if (!isConform) {
          logger.warn("Row {} doesn't conform to schema of {}", row, className)
        }
        isConform
      })
    }

    conform(row, schema)
  }
}
