package com.winwire.adobe.execution.jdbc

case class JdbcConnectionConfig(
                                 url: String,
                                 driver: String,
                                 user: String,
                                 password: String,
                                 fetchSize: String,
                                 extraOptions: Map[String, String] = Map.empty
                               )
