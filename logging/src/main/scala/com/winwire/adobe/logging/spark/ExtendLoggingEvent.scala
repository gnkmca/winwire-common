package com.winwire.adobe.logging.spark

import org.apache.log4j.spi.LoggingEvent

import scala.util.Try

private[logging] trait ExtendLoggingEvent {
  def extend(event: LoggingEvent): LoggingEvent = {
    val clusterName = getValue("CLUSTER_NAME")
    val appName = getValue("APP_NAME")

    val extendedMessage = s"cluster_name=$clusterName app_name=$appName ${event.getMessage}"

    new LoggingEvent(
      event.fqnOfCategoryClass,
      event.getLogger,
      event.getTimeStamp,
      event.getLevel,
      extendedMessage,
      Option(event.getThrowableInformation).map(_.getThrowable).orNull
    )
  }

  private def getValue(varName: String): String = Try(sys.env(varName)).getOrElse("")
}
