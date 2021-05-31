package com.winwire.adobe.common.config.yaml

import java.io.InputStream

import com.winwire.adobe.common.config.ConfigParser
import com.winwire.adobe.common.config.yaml.Constants.SYSTEM_TAG
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.reflect.{ClassTag, classTag}

private[config] class YamlConfigParser extends ConfigParser {

  protected def constructor: ConfigurableConsructor = {
    val constructor: ConfigurableConsructor = ConfigurableConsructor(getClass.getClassLoader)
    constructor.put(new Tag(SYSTEM_TAG), new SystemConstruct())
    constructor
  }

  protected def representer = new Representer()

  override def configFrom[A: ClassTag](stream: InputStream): A = {
    val yaml = new Yaml(constructor, representer)
    yaml.loadAs(stream, classTag[A].runtimeClass).asInstanceOf[A]
  }
}
