package com.winwire.adobe.common.config

class Secret(key: String, secrets: => SecretsStorage) {

  lazy val value: String = {
    secrets.get(key) match {
      case Some(value) => value
      case None => key
    }
  }
}
