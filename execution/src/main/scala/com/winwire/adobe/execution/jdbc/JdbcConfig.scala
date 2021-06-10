package com.winwire.adobe.execution.jdbc

import scala.beans.BeanProperty
import com.winwire.adobe.utils.KeyValue

class JdbcSection {
  @BeanProperty var url: String = _
  @BeanProperty var driver: String = _
  @BeanProperty var user: String = _
  @BeanProperty var password: String = _
  @BeanProperty var fetchSize: String = _
  @BeanProperty var extraOptions: Array[KeyValue] = Array.empty
}

trait JdbcConfig {
  @BeanProperty var jdbc: JdbcSection = _
}
