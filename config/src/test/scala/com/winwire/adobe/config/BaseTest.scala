package com.winwire.adobe.config

import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
abstract class BaseTest extends AnyFlatSpec with Matchers with MockFactory
