package com.winwire.adobe.execution

import java.sql.Timestamp
import java.time.Instant

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
abstract class BaseTest extends FlatSpec with Matchers {
  protected def now: Timestamp = Timestamp.from(Instant.now())
}
