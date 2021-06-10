package com.winwire.adobe.execution.jdbc

import com.winwire.adobe.execution.spark.SparkApplication

trait DefaultJdbc extends Jdbc {
  this: SparkApplication[_ <: JdbcConfig] =>

  override protected def jdbcConfig: JdbcConnectionConfig = {
    val conf = config.jdbc

    JdbcConnectionConfig(
      url = conf.url,
      driver = conf.driver,
      user = conf.user,
      password = conf.password,
      fetchSize = conf.fetchSize,
      extraOptions = conf.extraOptions.map(kv => (kv.key, kv.value)).toMap
    )
  }
}