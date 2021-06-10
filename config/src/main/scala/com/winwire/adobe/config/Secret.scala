package com.winwire.adobe.config
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
class Secret(key: String, secrets: => SecretsStorage) {

  lazy val value: String = {
    secrets.get(key) match {
      case Some(value) => value
      case None => key
    }
  }
}
