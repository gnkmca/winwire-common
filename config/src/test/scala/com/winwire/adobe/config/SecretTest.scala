package com.winwire.adobe.config

class SecretTest extends BaseTest {

  final val TEST_SECRET_KEY = "TEST_SECRET_KEY"
  final val TEST_SECRET_VALUE = "TEST_SECRET_VALUE"

  val secretsStorageMock: SecretsStorage = mock[SecretsStorage]

  "Secret" should "return data from provided secure storage by key" in {
    (secretsStorageMock.get(_: String)) expects TEST_SECRET_KEY once() returns Some(TEST_SECRET_VALUE)

    val secret = new Secret(TEST_SECRET_KEY, secretsStorageMock)

    secret.value should be(TEST_SECRET_VALUE)
  }

  "Secret" should "return key if exception occurred" in {
    (secretsStorageMock.get(_: String)) expects TEST_SECRET_KEY once() returns None

    val secret = new Secret(TEST_SECRET_KEY, secretsStorageMock)

    secret.value should be(TEST_SECRET_KEY)
  }
}
