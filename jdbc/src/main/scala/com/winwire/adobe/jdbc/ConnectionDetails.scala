package com.winwire.adobe.jdbc

case class ConnectionDetails(db: String, config: Map[String, String]) {

  val driver: String = config.getOrElse(s"$db.driver", config("jdbc.default.driver"))
  val url: String = config(s"$db.url")
  val user: String = config(s"$db.username")
  val pass: String = config(s"$db.password")

  override def toString: String = s"{db: $db, driver: $driver, url: $url, user: $user}"
}