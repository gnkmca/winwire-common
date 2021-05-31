/*package com.winwire.adobe.common.config

import java.net.URL

import org.scalatest.PrivateMethodTester

import scala.beans.BeanProperty
import scala.util.Failure

class TestConfig {
  @BeanProperty var test: String = _
  @BeanProperty var testInt: Int = _
}

class ConfigLoaderTest extends BaseTest with PrivateMethodTester {

  val loader: ConfigLoader = new ConfigLoader {}

  "ConfigLoader" should "load config config by path" in {

    val url: URL = getClass.getClassLoader.getResource("test.yaml")

    val expected = new TestConfig()
    expected.test = "test string"
    expected.testInt = 1

    val actual = loader.loadConfig[TestConfig](url.getPath).get

    actual.test should equal(expected.test)
    actual.testInt should equal(expected.testInt)
  }

  "ConfigLoader" should "return Failure if file does not exist" in {

    val expected = new TestConfig()
    expected.test = "test string"
    expected.testInt = 1

    val actual = loader.loadConfig[TestConfig]("test1.yaml")

    actual shouldBe a[Failure[_]]
  }

  "ConfigLoader" should "load config by path and use system properties" in {

    val url: URL = getClass.getClassLoader.getResource("test_system.yaml")
    System.setProperty("my.test", "test string11")

    val expected = new TestConfig()
    expected.test = "test string11"
    expected.testInt = 1

    val actual = loader.loadConfig[TestConfig](url.getPath).get

    actual.test should equal(expected.test)
    actual.testInt should equal(expected.testInt)
  }

}
*/