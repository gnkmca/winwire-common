package com.winwire.adobe.common.utils

import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.util.control.NonFatal

trait Using extends LazyLogging {

  def releaseSilently[A](resource: A)(releaseFunction: A => Unit): Unit = {
    try {
      if (resource != null) releaseFunction(resource)
    } catch {
      case e: Exception =>
        logger.error(s"Was not able to release resource: $resource.", e)
    }
  }

  def using[T <: AutoCloseable, V](resource: T)(function: T => V): V = {
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      function(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  private def closeAndAddSuppressed(e: Throwable, resource: AutoCloseable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }
}
