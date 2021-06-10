package com.winwire.adobe.config.yaml
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import com.winwire.adobe.config.{Secret, SecretsStorage}
import org.yaml.snakeyaml.constructor.AbstractConstruct
import org.yaml.snakeyaml.nodes.{Node, ScalarNode}

private[config] class SecretConstruct(storage: => SecretsStorage) extends AbstractConstruct {
  def construct(node: Node): AnyRef = {
    val scalarNode = node.asInstanceOf[ScalarNode]
    val value = scalarNode.getValue

    val secret = new Secret(value, storage)
    secret
  }
}
