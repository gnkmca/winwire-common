package com.winwire.adobe.common.config.yaml

import org.yaml.snakeyaml.constructor.{Construct, CustomClassLoaderConstructor}
import org.yaml.snakeyaml.nodes.Tag

private[config] case class ConfigurableConsructor(cLoader: ClassLoader)
  extends CustomClassLoaderConstructor(cLoader) {

  def put(tag: Tag, construct: Construct): Construct = {
    yamlConstructors.put(tag, construct)
  }

}
