package com.winwire.adobe.execution

import scala.collection.mutable

object TestConfigurator extends Configurator {
  def set(key: String, value: String)(implicit conf: mutable.Map[String, String]): Unit =
    setIfNotEmpty(key, value)(conf)
}

class ConfiguratorTest extends BaseTest {
  behavior of classOf[Configurator].getSimpleName

  it should "should set setting to config Map if it is not null" in {
    implicit val conf: mutable.Map[String, String] = mutable.Map.empty

    TestConfigurator.set("test", "test_value")

    conf.contains("test") shouldBe true
    conf.get("test") shouldBe Some("test_value")
  }

  it should "should not set setting to config Map if it is empty string" in {
    implicit val conf: mutable.Map[String, String] = mutable.Map.empty

    TestConfigurator.set("test", "")

    conf.contains("test") shouldBe true
    conf.get("test") shouldBe Some("")
  }

  it should "should not set setting to config Map if it is null" in {
    implicit val conf: mutable.Map[String, String] = mutable.Map.empty

    TestConfigurator.set("test", null)

    conf.contains("test") shouldBe false
  }
}
