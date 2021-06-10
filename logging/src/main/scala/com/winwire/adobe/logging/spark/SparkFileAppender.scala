package com.winwire.adobe.logging.spark

import org.apache.log4j.FileAppender
import org.apache.log4j.spi.LoggingEvent

class SparkFileAppender extends FileAppender with ExtendLoggingEvent {
  override def append(event: LoggingEvent): Unit = super.append(extend(event))
}

