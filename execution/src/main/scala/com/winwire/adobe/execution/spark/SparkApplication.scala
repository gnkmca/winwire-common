package com.winwire.adobe.execution.spark
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import com.winwire.adobe.execution.Application
import org.apache.spark.sql.SparkSession

import scala.reflect.{ClassTag, classTag}

abstract class SparkApplication[C: ClassTag] extends Application[C]
  with Spark
  with SparkSessionProvider
  with SparkConfigurator {

  implicit lazy val spark: SparkSession = getOrCreateSparkSession(name)

  override val configTag: ClassTag[C] = classTag[C]

  override protected def cleanup(): Unit = {
    logger.info("Job cleanup done")
  }

  override def run(): Unit = {
    configure(spark)
    super.run()
  }
}
