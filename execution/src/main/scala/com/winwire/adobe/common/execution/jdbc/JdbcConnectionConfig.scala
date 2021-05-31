package com.winwire.adobe.common.execution.jdbc

case class JdbcConnectionConfig(
                                 url: String,
                                 driver: String,
                                 user: String,
                                 password: String,
                                 fetchSize: String
                               )
