package com.winwire.adobe.common.config

import java.io.InputStream

import scala.reflect.ClassTag

private[config] trait ConfigParser {
  def configFrom[A: ClassTag](stream: InputStream): A
}
