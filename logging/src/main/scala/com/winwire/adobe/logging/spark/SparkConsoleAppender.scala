package com.winwire.adobe.logging.spark

import org.apache.log4j.ConsoleAppender
import org.apache.log4j.spi.LoggingEvent

class SparkConsoleAppender extends ConsoleAppender with ExtendLoggingEvent {
  override def append(event: LoggingEvent): Unit = super.append(extend(event))
}
