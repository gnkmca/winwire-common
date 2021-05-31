package com.winwire.adobe.common.execution.spark

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.winwire.adobe.common.execution.BaseTest

case class Test(name: String, value: String, testInt: Int)

class SchemaMapperTest extends BaseTest with DatasetSuiteBase {
  behavior of classOf[SchemaMapper].getSimpleName

  private val source = List(Test("test_name", "test_value", 1))
  private val mapper = new SchemaMapper {}

  import spark.implicits._

  it should "rename one column if only one passed to method" in {
    val mappings = Map("value" -> "newValue")
    val ds = source.toDS()
    val expected = ds.withColumnRenamed("value", "newValue")

    val actual = mapper.updateSchema(ds, mappings)

    assertDataFrameEquals(expected, actual)
  }

  it should "rename three columns if all three are passed to method" in {
    val mappings = Map(
      "name" -> "newName",
      "value" -> "newValue",
      "testInt" -> "newInt"
    )
    val ds = source.toDS()
    val expected = ds
      .withColumnRenamed("name", "newName")
      .withColumnRenamed("value", "newValue")
      .withColumnRenamed("testInt", "newInt")

    val actual = mapper.updateSchema(ds, mappings)

    assertDataFrameEquals(expected, actual)
  }

  it should "not do anything if mappings are empty" in {
    val mappings: Map[String, String] = Map()
    val ds = source.toDS()
    val expected = ds.toDF()

    val actual = mapper.updateSchema(ds, mappings)

    assertDataFrameEquals(expected, actual)
  }

  it should "not do anything if specified column does not exist in source dataset" in {
    val mappings: Map[String, String] = Map("testNotExists" -> "newTest")
    val ds = source.toDS()
    val expected = ds.toDF()

    val actual = mapper.updateSchema(ds, mappings)

    assertDataFrameEquals(expected, actual)
  }

  it should "change column name for input DataFrame" in {
    val mappings: Map[String, String] = Map("name" -> "newName")
    val df = source.toDF()
    val expected = df.withColumnRenamed("name", "newName")

    val actual = mapper.updateSchema(df, mappings)

    assertDataFrameEquals(expected, actual)
  }
}
