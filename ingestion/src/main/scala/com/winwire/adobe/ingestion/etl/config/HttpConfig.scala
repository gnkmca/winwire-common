package com.winwire.adobe.ingestion.etl.config

import scala.beans.BeanProperty

class ProxyConfig {
  @BeanProperty var proxyHost: String = _
  @BeanProperty var proxyPort: String = _
}

class HttpConfig {
  @BeanProperty var proxy: ProxyConfig = _
  @BeanProperty var connectTimeout: String = _
  @BeanProperty var readTimeout: String = _
  @BeanProperty var writeTimeout: String = _
  @BeanProperty var username: String = _
  @BeanProperty var password: String = ""
}
