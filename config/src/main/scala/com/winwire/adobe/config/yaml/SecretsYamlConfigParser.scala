
package com.winwire.adobe.config.yaml
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import java.io.InputStream

import com.winwire.adobe.config.{ContainsSecrets, Secret, SecretsStorage}
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.reflect.{ClassTag, classTag}

private[config] class SecretsYamlConfigParser(secretsStorage: ContainsSecrets => SecretsStorage)
  extends YamlConfigParser {

  import Constants._

  private var basicConfig: ContainsSecrets = _
  private lazy val secrets: SecretsStorage = secretsStorage(basicConfig)

  override protected def constructor: ConfigurableConsructor = {
    val constructor: ConfigurableConsructor = super.constructor
    constructor.put(new Tag(SECRET_TAG), new SecretConstruct(secrets))
    constructor
  }

  override protected def representer: Representer = {
    val representer = super.representer
    representer.addClassTag(classOf[Secret], new Tag(SECRET_TAG))
    representer
  }

  override def configFrom[A: ClassTag](stream: InputStream): A = {
    assert(classOf[ContainsSecrets].isAssignableFrom(classTag[A].runtimeClass))

    val config = super.configFrom(stream)
    basicConfig = config.asInstanceOf[ContainsSecrets]
    config
  }
}
