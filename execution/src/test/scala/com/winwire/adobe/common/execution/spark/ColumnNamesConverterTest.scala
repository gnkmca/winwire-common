package com.winwire.adobe.common.execution.spark

import com.winwire.adobe.common.execution.{BaseTest, SparkSessionProviderForTest}
import org.apache.spark.sql.SparkSession

class ColumnNamesConverterTest extends BaseTest with SparkSessionProviderForTest{

  import ColumnNamesConverter._

  val spark: SparkSession = getOrCreateSparkSession("columns_converter")

  import spark.implicits._

  "toCamel" should "correctly covert snake case column names with letters only" in {
    val df = List(("test", "test")).toDF("test_column", "second_test_column")

    val actual = df.toCamel

    actual.columns.length shouldBe 2
    actual.columns(0) shouldBe "testColumn"
    actual.columns(1) shouldBe "secondTestColumn"
  }

  "toCamel" should "correctly covert snake case column names with letters digits" in {
    val df = List(("test", "test")).toDF("test_column_2", "second2_test_column")

    val actual = df.toCamel

    actual.columns.length shouldBe 2
    actual.columns(0) shouldBe "testColumn2"
    actual.columns(1) shouldBe "second2TestColumn"
  }

  "toSnake" should "correctly covert camel case column names with letters only" in {
    val df = List(("test", "test")).toDF("testColumn", "secondTestColumn")

    val actual = df.toSnake

    actual.columns.length shouldBe 2
    actual.columns(0) shouldBe "test_column"
    actual.columns(1) shouldBe "second_test_column"
  }

  "toSnake" should "correctly covert snake case column names with letters digits" in {
    val df = List(("test", "test")).toDF("testColumn2", "second2TestColumn")

    val actual = df.toSnake

    actual.columns.length shouldBe 2
    actual.columns(0) shouldBe "test_column2"
    actual.columns(1) shouldBe "second2_test_column"
  }
}
