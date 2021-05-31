package com.winwire.adobe.common.execution.spark

import org.apache.spark.sql.SparkSession

trait SparkConfigurator {
  def configure(spark: SparkSession): Unit = {}
}
