package com.winwire.adobe.jdbc

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JdbcUtilsTest extends FlatSpec with Matchers {
  behavior of JdbcUtils.getClass.getSimpleName

  it should "concatenate strings" in {
    val inputData = Seq("city", "state", "region")
    val currentResult = JdbcUtils.concatenateStrings(inputData)
    val expectedResult = "city, state, region"
    currentResult shouldBe expectedResult
  }

  it should "convert scala types to sql types" in {
    val inputData = Seq("USA", 1.4f, 123.4, 5453L, true)
    val currentResult = JdbcUtils.toSqlTypes(inputData).toSet
    val expectedResult = Set("'USA'", "1.4", "123.4", "5453", "1")
    currentResult shouldBe expectedResult
  }

  it should "parse case class into Map[String, Any]" in {
    case class Student(id: Int, name: String, year: Int)
    val inputData = Student(1, "Person", 2020)
    val currentResult = JdbcUtils.getCaseClassParams[Student](inputData, Some("id"))
    val expectedResult = Map[String, Any]("name" -> "Person", "year" -> 2020)

    val diff = (currentResult.keySet -- expectedResult.keySet) ++ (expectedResult.keySet -- currentResult.keySet)

    diff.size shouldBe 0
  }
}