package com.winwire.adobe.common.execution

import scala.collection.mutable

trait Configurator {
  protected def setIfNotEmpty(key: String, value: String)
                             (implicit conf: mutable.Map[String, String]): Unit =
    setIfNotEmpty(key, Option(value))(conf)

  protected def setIfNotEmpty(key: String, value: Option[String])
                             (implicit conf: mutable.Map[String, String]): Unit = {
    value match {
      case Some(v) => conf += (key -> v)
      case None =>
    }
  }
}
