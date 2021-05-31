package com.winwire.adobe.common.execution

import com.winwire.adobe.common.execution.spark.SparkSessionProvider
import org.apache.spark.SparkConf

trait SparkSessionProviderForTest extends SparkSessionProvider {
  override protected def sparkConfiguration: SparkConf = {
    val conf = super.sparkConfiguration
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    conf
  }
}
