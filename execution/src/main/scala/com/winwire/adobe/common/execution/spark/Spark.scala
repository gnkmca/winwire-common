package com.winwire.adobe.common.execution.spark

import org.apache.spark.sql.SparkSession

trait Spark {
  val spark: SparkSession
}
