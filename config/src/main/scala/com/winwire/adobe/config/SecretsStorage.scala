package com.winwire.adobe.config
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
trait SecretsStorage {
  def get(name: String): Option[String]
}