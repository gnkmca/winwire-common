package com.winwire.adobe.jdbc

import java.sql.ResultSet
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

case class UserProfile(id: Int, first_name: String, last_name: String, age: Int)
case class Person(name: String, age: Int)

@RunWith(classOf[JUnitRunner])
class JdbcClientTest extends JdbcTestConfig {
  behavior of classOf[JdbcClient].getSimpleName

  val tableName = "user_profiles"
  val jdbcClient = new JdbcClient(connectionDetails = connectionDetails)

  "updateSQL" should "execute create statement" in {
    val statement =
      s"""CREATE TABLE $DB.$tableName (
         |id INT IDENTITY,
         |first_name VARCHAR(50) not null,
         |last_name VARCHAR(50) not null,
         |age INT not null)""".stripMargin
    val affectedRows = jdbcClient.updateSQL(statement, returnKey = false)

    affectedRows.getOrElse(-1) shouldBe 0
  }

  "insert" should "insert case class into table" in {
    case class UserProfile(id: Int, first_name: String, last_name: String, age: Int)
    val data = UserProfile(1, "First Name", "Last Name", 11)
    val generatedId = jdbcClient.insert(data, tableName)

    generatedId.getOrElse(-1) shouldBe 1
  }

  "select" should "select data into case class" in {
    val resultSetTransformation = (rs: ResultSet) => {
      val id = rs.getInt("id")
      val firstName = rs.getString("first_name")
      val lastName = rs.getString("last_name")
      val age = rs.getInt("age")
      UserProfile(id, firstName, lastName, age)
    }
    val currentObject = jdbcClient.select[UserProfile](tableName)(resultSetTransformation).head
    val expectedObject = UserProfile(1, "First Name", "Last Name", 11)

    currentObject shouldBe expectedObject
  }

  "executeSQL" should "execute SQL statement with result" in {
    val statement = s"SELECT first_name, last_name, age FROM $DB.$tableName WHERE id = 1"
    val resultSetTransformation = (rs: ResultSet) => {
      val firstName = rs.getString("first_name")
      val lastName = rs.getString("last_name")
      val age = rs.getInt("age")
      Person(s"$firstName $lastName", age)
    }
    val currentObject = jdbcClient.executeSQL[Person](statement)(resultSetTransformation).head
    val expectedObject = Person("First Name Last Name", 11)
    currentObject shouldBe expectedObject
  }

}
