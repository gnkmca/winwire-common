package com.winwire.adobe.execution
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import com.typesafe.scalalogging.slf4j.LazyLogging

trait ExecutableJob[C] extends Job[C] with LazyLogging {

  def run(): Unit = {

    logger.info(s"Executing $name job")

    try {
      script()
      logger.info(s"Job $name successfully finished")
    }
    catch {
      case e: Exception =>
        logger.error(s"Job $name finished with errors", e)
        throw e
    } finally {
      cleanup()
    }
  }

  protected def cleanup(): Unit = {}
}
