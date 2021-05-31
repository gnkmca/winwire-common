
package com.winwire.adobe.common.config

import java.io.{File, FileInputStream}

//import com.winwire.adobe.common.config.yaml.{SecretsYamlConfigParser, YamlConfigParser}

import com.winwire.adobe.common.config.yaml.{YamlConfigParser}

import scala.reflect._
import scala.util.{Failure, Success, Try}

trait ContainsSecrets

trait ConfigLoader //extends SecretsStorageProvider
{

  def loadConfig[A: ClassTag](path: String): Try[A] = {
    val configFile = loadFile(path)

    configFile match {
      case Success(file) =>
        Try {
          val parser = parserFor[A]
          parser.configFrom(file)
        }

      case Failure(e) => Failure(e)
    }
  }

  private[this] def loadFile(path: String): Try[FileInputStream] = Try {
    val file = new File(path)
    new FileInputStream(file)
  }

  private[this] def parserFor[A: ClassTag]: ConfigParser = {
    val classA = classTag[A].runtimeClass
/*
    if (classOf[ContainsSecrets].isAssignableFrom(classA))
      //new SecretsYamlConfigParser(c => secretsStorage(c))
      new YamlConfigParser(c => Yaml(c))
    else*/
      new YamlConfigParser
  }
}
