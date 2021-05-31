package com.winwire.adobe.common.execution.spark

import java.net.URL

import com.winwire.adobe.common.execution.{BaseTest, SparkSessionProviderForTest, TestConfig}

import scala.reflect._

class TestSparkApplication extends SparkApplication[TestConfig] with SparkSessionProviderForTest {
  override val configTag: ClassTag[TestConfig] = classTag[TestConfig]

  override def name = "spark"

  override def script(): Unit = {}
}

class SparkApplicationTest extends BaseTest {
  val url: URL = getClass.getClassLoader.getResource("test.yaml")

  "SparkApplication" should "create a sparkSession object" in {
    val app = new TestSparkApplication()
    app.main(Array("--config", url.getPath))

    app.spark should not be null
  }

}
