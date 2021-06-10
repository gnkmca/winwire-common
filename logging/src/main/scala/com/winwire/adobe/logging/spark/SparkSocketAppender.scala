package com.winwire.adobe.logging.spark

import org.apache.log4j.net.SocketAppender
import org.apache.log4j.spi.LoggingEvent

class SparkSocketAppender extends SocketAppender with ExtendLoggingEvent {
  override def append(event: LoggingEvent): Unit = super.append(extend(event))
}