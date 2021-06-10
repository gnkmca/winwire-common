package com.winwire.adobe.execution

import java.io.FileNotFoundException
import java.net.URL

import com.typesafe.scalalogging.slf4j.Logger
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.slf4j.{Logger => UnderlyingLogger}

import scala.reflect.{ClassTag, classTag}

class ApplicationTest extends BaseTest with MockitoSugar {

  var scriptCalled: Boolean = false

  val mockLogger: UnderlyingLogger = mock[UnderlyingLogger]
  when(mockLogger.isInfoEnabled).thenReturn(true)
  when(mockLogger.isErrorEnabled).thenReturn(true)

  trait TextExecutableJob extends ExecutableJob[TestConfig] {
    override def name: String = "test"

    override def script(): Unit = {
      scriptCalled = true
    }
  }

  private def createApp: Application[TestConfig] with TextExecutableJob = {
    new Application[TestConfig] with TextExecutableJob {
      override protected lazy val logger: Logger = Logger(mockLogger)
      override val configTag: ClassTag[TestConfig] = classTag[TestConfig]
    }
  }

  val url: URL = getClass.getClassLoader.getResource("test.yaml")

  "Application" should "call script method" in {
    val app: Application[TestConfig] with TextExecutableJob = createApp

    app.main(Array("--config", url.getPath))

    scriptCalled shouldBe true
  }

  "Application" should "correctly parse config" in {
    val app: Application[TestConfig] with TextExecutableJob = createApp

    app.main(Array("--config", url.getPath))
    val actual: TestConfig = app.config

    actual.test shouldBe "test string"
    actual.testInt shouldBe 1
  }

  "Application" should "correctly log message in case of failure" in {
    val app: Application[TestConfig] with TextExecutableJob = createApp

    app.main(Array("--config", "test2.yaml"))

    verify(mockLogger).error(
      ArgumentMatchers.eq("Failed to create TestConfig object from arguments: --config,test2.yaml."),
      ArgumentMatchers.any(classOf[FileNotFoundException]))
  }

  "Application" should "correctly log message in case of failure [with verbose]" in {
    val app: Application[TestConfig] with TextExecutableJob = createApp
    var exceptionThrown = false

    try {
      app.main(Array("--verbose", "--config", "test2.yaml"))
    }
    catch {
      case _: Exception => exceptionThrown = true
    }

    exceptionThrown shouldBe true

    verify(mockLogger).error(
      ArgumentMatchers.eq("Failed to create TestConfig object from arguments: --config,test2.yaml."),
      ArgumentMatchers.any(classOf[FileNotFoundException]))
  }
}
