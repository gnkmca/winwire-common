package com.winwire.adobe.common.execution.jdbc

import com.winwire.adobe.common.execution.spark.Spark
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Jdbc extends Spark {

  protected def jdbcConfig: JdbcConnectionConfig

  private[this] lazy val conf = jdbcConfig

  def readFromJdbc(statement: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", conf.url)
      .option("driver", conf.driver)
      .option("user", conf.user)
      .option("password", conf.password)
      .option("fetchSize", conf.fetchSize)
      .option("dbtable", statement)
      .load()
  }
}

object Jdbc {
  def apply(config: JdbcConnectionConfig)(implicit ss: SparkSession): Jdbc =
    new Jdbc {
      val spark: SparkSession = ss

      protected def jdbcConfig: JdbcConnectionConfig = config
    }
}
