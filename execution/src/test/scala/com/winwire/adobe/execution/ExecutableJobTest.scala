package com.winwire.adobe.execution

import com.typesafe.scalalogging.slf4j.Logger
import org.mockito.MockitoSugar
import org.slf4j.{Logger => UnderlyingLogger}

class ExecutableJobTest extends BaseTest with MockitoSugar {
  var mockLogger: UnderlyingLogger = _

  "ExecutableJob" should "run provided job without exceptions" in {
    val job = createJob
    var exceptionThrown = false
    try {
      job.run()
    }
    catch {
      case _: Exception => exceptionThrown = true
    }
    exceptionThrown should be(false)
  }

  "ExecutableJob" should "run provided job with exceptions" in {
    val ex = new Exception
    val job = createFailingJob(ex)
    var exceptionThrown = false
    try {
      job.run()
    }
    catch {
      case _: Exception => exceptionThrown = true
    }
    exceptionThrown should be(true)
  }

  "ExecutableJob" should "log message when job's execution started" in {
    val job = createJob

    job.run()

    verify(mockLogger).info(s"Executing test job")
  }

  "ExecutableJob" should "log message when job's execution finished with Success result" in {
    val job = createJob

    job.run()

    verify(mockLogger).info("Job test successfully finished")
  }

  "ExecutableJob" should "log message when job's execution finished with Failure result" in {
    val ex = new Exception
    val job = createFailingJob(ex)

    try {
      job.run()
    }
    catch {
      case _: Exception =>
    }

    verify(mockLogger).error("Job test finished with errors", ex)
  }

  "ExecutableJob" should "call mainStep during execution" in {
    val job = createJob

    job.run()

    job.scriptCalled should be(true)
  }

  "ExecutableJob" should "call cleanup after execution" in {
    val job = createJob

    job.run()

    job.cleanupCalled should be(true)
  }

  private def createFailingJob(ex: Exception): TestExecutableJob = {
    new TestExecutableJob(Logger(mockLogger), ex)
  }

  private def createJob: TestExecutableJob = {
    mockLogger = mock[UnderlyingLogger]
    when(mockLogger.isInfoEnabled).thenReturn(true)
    when(mockLogger.isErrorEnabled).thenReturn(true)

    new TestExecutableJob(Logger(mockLogger), null)
  }

  class TestExecutableJob(log: Logger, ex: Exception = null) extends ExecutableJob[String] {
    override lazy val logger: Logger = log

    var scriptCalled: Boolean = false
    var cleanupCalled: Boolean = false

    override def name: String = "test"

    override def script(): Unit = {
      if (ex != null)
        throw ex
      scriptCalled = true
    }

    override def cleanup(): Unit = {
      cleanupCalled = true
    }

    override val config: String = "test"
  }
}
