/*package com.winwire.adobe.config

import java.net.URL

import com.winwire.adobe.config.azure.AzureKVConfig
import org.scalatest.PrivateMethodTester

import scala.beans.BeanProperty

class TestConfigSecretConfig extends AzureKVConfig {
  @BeanProperty var test: String = _
  @BeanProperty var testInt: Int = _
  @BeanProperty var secret: Secret = _
}

class ConfigLoaderIntegrationTest extends BaseTest with PrivateMethodTester {

  val loader: ConfigLoader = new ConfigLoader {}

  /**
   * It is an integration test that works directly with Azure Kev Vault.
   * TEST_SECRET_VALUE should be present in Azure KV
   */
  "ConfigLoader" should "use SecretsYamlConfigParser for configs which contain secrets" in {
    val expected = new TestConfigSecretConfig()
    expected.test = "test string"
    expected.testInt = 1
    expected.secret = new Secret("testsecret", null)

    val url: URL = getClass.getClassLoader.getResource("test_secret.yaml")

    val actual = loader.loadConfig[TestConfigSecretConfig](url.getPath).get

    actual.test should equal(expected.test)
    actual.testInt should equal(expected.testInt)
    //actual.secret.value should equal("TEST_SECRET_VALUE")
    actual.secret.value should equal("testsecret")
  }
}
*/