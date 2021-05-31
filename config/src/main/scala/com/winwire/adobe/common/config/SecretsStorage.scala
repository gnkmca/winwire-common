package com.winwire.adobe.common.config

trait SecretsStorage {
  def get(name: String): Option[String]
}