package com.winwire.adobe.execution.spark
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import org.apache.spark.sql.SparkSession

trait SparkConfigurator {
  def configure(spark: SparkSession): Unit = {}
}
