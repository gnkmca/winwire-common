package com.winwire.adobe.execution
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import com.winwire.adobe.config.ConfigLoader
import com.winwire.adobe.utils.Using
import scopt.OptionParser

import scala.reflect._
import scala.util.{Failure, Success, Try}

final case class AppArguments(configPath: String = "", verbose: Boolean = false)

trait Application[C] extends ExecutableJob[C] with ConfigLoader with Using {
  val configTag: ClassTag[C]

  private[this] val jobClassName = getClass.getSimpleName
  private[this] var conf: C = _

  def main(args: Array[String]): Unit = {
    val appArguments = parse(args)

    val executeJob = (appArguments: AppArguments) => parseConfig(appArguments) match {
      case Success(config) =>
        conf = config
        run()
      case Failure(e) =>
        val argsString = args.mkString(",")
        val configClassName = configTag.runtimeClass.getSimpleName
        logger.error(s"Failed to create $configClassName object from arguments: $argsString.", e)
        throw e
    }

    if (appArguments.verbose) {
      executeJob(appArguments)
    } else {
      releaseSilently(appArguments)(executeJob)
    }
  }

  private def parseConfig(args: AppArguments): Try[C] = loadConfig[C](args.configPath)(configTag)

  private def parse(args: Array[String]): AppArguments = {
    val parser = new OptionParser[AppArguments](jobClassName) {
      head("scopt", "3.x")

      opt[Unit]('v', "verbose")
        .optional()
        .action((_, parameters) => parameters.copy(verbose = true))
        .text("Don't suppress any errors")

      opt[String]('c', "config")
        .required()
        .action((path, parameters) => parameters.copy(configPath = path))
        .text("Path to config file")
    }
    parser.parse(args, AppArguments()) match {
      case Some(value) => value
      case None => throw new IllegalArgumentException("Failed to parse arguments")
    }
  }

  override def config: C = conf
}
