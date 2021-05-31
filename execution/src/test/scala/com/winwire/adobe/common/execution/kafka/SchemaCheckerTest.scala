/*
package com.winwire.adobe.common.execution.kafka

import java.sql.{Date,Timestamp}

import com.winwire.adobe.common.execution.BaseTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

class SchemaCheckerTest extends BaseTest {

  def row(values: Array[Any], schema: StructType): Row = {
    new GenericRowWithSchema(values, schema)
  }

  "SchemaChecker" should "conform equal schemas" in {
    val structType: StructType = StructType(Seq(StructField("a", BooleanType)))
    val checker = new SchemaChecker[S](structType)
    val conform = checker.conform(row(Array(true), StructType(Seq(StructField("a", BooleanType)))))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with missing field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", BooleanType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val actualSchema: StructType = StructType(Seq(StructField("b", BooleanType)))
    val conform = checker.conform(row(Array("s"), actualSchema))
    conform should equal(false)
  }

  "SchemaChecker" should "not conform row with null in not nullable field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", BooleanType, nullable = false)))
    val checker = new SchemaChecker[S](expectedSchema)

    val actualSchema: StructType = StructType(Seq(StructField("a", BooleanType)))
    val conform = checker.conform(row(Array(null), actualSchema))
    conform should equal(false)
  }

  "SchemaChecker" should "conform row with Boolean value in Boolean field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", BooleanType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(false), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not Boolean value in Boolean field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", BooleanType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array("asd"), expectedSchema))
    conform should equal(false)
  }

  "SchemaChecker" should "conform row with String value in String field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", StringType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array("SomeString"), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not String value in String field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", BooleanType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(10), expectedSchema))
    conform should equal(false)
  }

  "SchemaChecker" should "conform row with Byte value in Byte field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", ByteType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(1 toByte), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not Byte value in Byte field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", ByteType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conformString = checker.conform(row(Array("asd"), expectedSchema))
    conformString should equal(false)

    val conformShort = checker.conform(row(Array(257), expectedSchema))
    conformShort should equal(false)
  }

  "SchemaChecker" should "conform row with Short value in Short field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", ShortType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(257 toShort), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not Short value in Short field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", ShortType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conformString = checker.conform(row(Array("asd"), expectedSchema))
    conformString should equal(false)

    val conformInt = checker.conform(row(Array(70000), expectedSchema))
    conformInt should equal(false)
  }

  "SchemaChecker" should "conform row with Integer value in Integer field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", IntegerType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(70000), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not Integer value in Integer field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", IntegerType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conformString = checker.conform(row(Array("asd"), expectedSchema))
    conformString should equal(false)
  }

  "SchemaChecker" should "conform row with Long value in Long field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", LongType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(123124L), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not Long value in Long field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", LongType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array("asd"), expectedSchema))
    conform should equal(false)
  }

  "SchemaChecker" should "conform row with Float value in Float field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", FloatType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(123.4f), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not Float value in Float field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", FloatType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array("asd"), expectedSchema))
    conform should equal(false)
  }

  "SchemaChecker" should "conform row with Double value in Double field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", DoubleType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(12435.31d), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not Double value in Double field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", DoubleType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array("asd"), expectedSchema))
    conform should equal(false)
  }

  "SchemaChecker" should "conform row with Date value in Date field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", DateType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(new Date(System.currentTimeMillis())), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not Date value in Date field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", DateType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array("asd"), expectedSchema))
    conform should equal(false)
  }

  "SchemaChecker" should "conform row with Timestamp value in Timestamp field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", TimestampType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array(new Timestamp(System.currentTimeMillis())), expectedSchema))
    conform should equal(true)
  }

  "SchemaChecker" should "not conform row with not Timestamp value in Timestamp field" in {
    val expectedSchema: StructType = StructType(Seq(StructField("a", TimestampType)))
    val checker = new SchemaChecker[S](expectedSchema)

    val conform = checker.conform(row(Array("asd"), expectedSchema))
    conform should equal(false)
  }

  /*
                        case s: StructType => conform(row.getStruct(index), s)
   */

}

case class S(justAField: String)

 */