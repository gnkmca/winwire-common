package com.winwire.adobe.config.yaml
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import org.yaml.snakeyaml.constructor.{Construct, CustomClassLoaderConstructor}
import org.yaml.snakeyaml.nodes.Tag

private[config] case class ConfigurableConsructor(cLoader: ClassLoader)
  extends CustomClassLoaderConstructor(cLoader) {

  def put(tag: Tag, construct: Construct): Construct = {
    yamlConstructors.put(tag, construct)
  }

}
