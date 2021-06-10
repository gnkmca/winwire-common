package com.winwire.adobe.jdbc

import java.sql.{CallableStatement, ResultSet, SQLType}
import com.winwire.adobe.jdbc.JdbcConnector.ConnectionManager
import com.winwire.adobe.jdbc.JdbcUtils._
import scala.reflect.runtime.universe.{TypeTag, MethodSymbol}

class JdbcClient(connectionDetails: ConnectionDetails) {

  def insert[T](entity: T, tableName: String, returnGeneratedKey: Boolean = false, fieldToExclude: Option[String] = None): Option[Long] = {
    val fieldsWithValues = getCaseClassParams[T](entity, fieldToExclude)
    val fields = concatenateStrings(fieldsWithValues.keys)
    val values = concatenateStrings(toSqlTypes(fieldsWithValues.values))
    val statement = s"INSERT INTO ${connectionDetails.db}.$tableName ($fields) VALUES ($values);"
    updateSQL(statement, returnGeneratedKey)
  }

  def select[T: TypeTag](tableName: String, filterMap: Map[String, Any] = Map.empty)(transform: ResultSet => T): Seq[T] = {
    val tag = implicitly[TypeTag[T]]
    val selectExpression = concatenateStrings(extractFields(tag))

    val filterFields = filterMap.keys
    val filter = filterFields.map(field => {
      val filterValue = toSqlType(filterMap(field))
      s"$field=$filterValue"
    }).mkString(" AND ")
    val pureSelect = s"SELECT $selectExpression FROM ${connectionDetails.db}.$tableName"
    val statement = if (filterMap.nonEmpty) pureSelect + s" WHERE $filter;" else pureSelect
    executeSQL[T](statement)(transform)
  }

  def executeSQL[T](statement: String)(transform: ResultSet => T): Seq[T] = {
    val connectionManager = ConnectionManager(Set(connectionDetails))
    val data = connectionManager
      .withConnectionTo(connectionDetails)
      .sql(statement)(transform)

    connectionManager.close()
    data
  }

  def updateSQL(statement: String, returnKey: Boolean): Option[Long] = {
    val connectionManager = ConnectionManager(Set(connectionDetails))
    val data = connectionManager
      .withConnectionTo(connectionDetails)
      .sqlUpdate(statement, returnKey)
    connectionManager.close()
    data
  }

  /**
   * Execute Stored Procedure with Input Params
   *
   * @param statement
   * @param inputParams
   */
  def executeStoredProcedure(statement: String, inputParams: Map[String,String], outputParams: Map[String, Int]): (CallableStatement, ConnectionManager) = {
    val connectionManager = ConnectionManager(Set(connectionDetails))
    (connectionManager
      .withConnectionTo(connectionDetails)
      .executeStoredProcedure(statement, inputParams, outputParams),connectionManager)
  }

  private[jdbc] def extractFields[T](tag: TypeTag[T]): Seq[String] = {
    tag.tpe.members.collect { case m: MethodSymbol if m.isCaseAccessor => m.name.toString }.toSeq
  }
}
