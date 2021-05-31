/*package com.winwire.adobe.common.config.yaml

import com.winwire.adobe.common.config.{Secret, SecretsStorage}
import org.yaml.snakeyaml.constructor.AbstractConstruct
import org.yaml.snakeyaml.nodes.{Node, ScalarNode}

private[config] class SecretConstruct(storage: => SecretsStorage) extends AbstractConstruct {
  def construct(node: Node): AnyRef = {
    val scalarNode = node.asInstanceOf[ScalarNode]
    val value = scalarNode.getValue

    val secret = new Secret(value, storage)
    secret
  }
}*/
