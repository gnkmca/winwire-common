package com.winwire.adobe.logging.spark

import org.apache.log4j.RollingFileAppender
import org.apache.log4j.spi.LoggingEvent

class SparkRollingFileAppender extends RollingFileAppender with ExtendLoggingEvent {
  override def append(event: LoggingEvent): Unit = super.append(extend(event))
}
