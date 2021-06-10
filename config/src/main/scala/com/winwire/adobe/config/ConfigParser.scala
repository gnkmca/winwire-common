package com.winwire.adobe.config
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import java.io.InputStream

import scala.reflect.ClassTag

private[config] trait ConfigParser {
  def configFrom[A: ClassTag](stream: InputStream): A
}
