package com.winwire.adobe.ingestion.etl.util

object Params {

  def insertParams(pattern: String, params: Map[String, String]): String = {
    val values = params.filter(p => p._2 != null)
    values.foldLeft(pattern) {
      case (query, (key, value)) => query.replace(s"$${${key}}", value)
    }
  }
}
