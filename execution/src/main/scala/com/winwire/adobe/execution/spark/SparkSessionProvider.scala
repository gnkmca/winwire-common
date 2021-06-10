package com.winwire.adobe.execution.spark
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
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
