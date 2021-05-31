package com.winwire.adobe.common.execution.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  protected def sparkConfiguration: SparkConf = {
    new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .set("spark.sql.caseSensitive", "false")
  }

  protected def getOrCreateSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .config(sparkConfiguration)
      .getOrCreate()
  }
}
