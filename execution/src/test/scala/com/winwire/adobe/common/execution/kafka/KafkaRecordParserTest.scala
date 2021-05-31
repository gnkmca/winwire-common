/*
package com.winwire.adobe.common.execution.kafka

import java.sql.Timestamp

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.winwire.adobe.common.execution.BaseTest
import com.winwire.adobe.common.execution.spark.SparkApplication
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.mockito.MockitoSugar

class KafkaRecordParserTest extends BaseTest
  with MockitoSugar
  with DatasetSuiteBase {

  case class TestApp(override val name: String = "Test app") extends SparkApplication[String] with KafkaRecordParser {
    override def script(): Unit = {}
  }

  "KafkaRecordParser parseValue" should "parse KafkaRecord value to case class" in {
    import spark.implicits._

    val input = List(KafkaRecord("key",
      """
        |{
        |"intValue":123,
        |"strValue":"value"
        |}
        |""".stripMargin,
      now))

    val app = TestApp()
    implicit val sp: SparkSession = app.spark
    val actual: Dataset[SimpleValue] = app.datasetFromKafkaRecords[SimpleValue](input.toDS())

    val expected = List(SimpleValue(123, "value"))
    assertDatasetEquals(expected.toDS(), actual)
  }

  "KafkaRecordParser parseValueToDf" should "parse KafkaRecord value to DataFrame" in {
    import spark.implicits._

    val timestamp = now

    val input = List(KafkaRecord("key",
      """
        |{
        |"intValue":123,
        |"strValue":"value"
        |}
        |""".stripMargin,
      timestamp))

    val app = TestApp()
    val actual: DataFrame = app.dataframeFromKafkaRecords(input.toDS())(app.spark)

    val expected = List(SimpleValueWithTimestamp(123, "value", timestamp))
    assertDatasetEquals(expected.toDF(), actual)
  }

  "KafkaRecordParser parseValue" should "filter out non compatible KafkaRecords to case class" in {
    import spark.implicits._

    val input = List(
      KafkaRecord("key",
        "\n",
        now),
      KafkaRecord("key",
        "\r\n",
        now),
      KafkaRecord("key",
        "{}",
        now),
      KafkaRecord("key",
        """
          |{
          |"strValue":"value"
          |}
          |""".stripMargin,
        now),
      KafkaRecord("key",
        """
          |{
          |"intValue":123,
          |"strValue":"value"
          |}
          |""".stripMargin,
        now))

    val app = TestApp()
    implicit val sp: SparkSession = app.spark
    val actual: Dataset[SimpleValue] = app.datasetFromKafkaRecords[SimpleValue](input.toDS())

    val expected = List(SimpleValue(123, "value"))
    assertDatasetEquals(expected.toDS(), actual)
  }

}

case class SimpleValue(intValue: Int, strValue: String)

case class SimpleValueWithTimestamp(intValue: Int, strValue: String, timestamp: Timestamp)


 */