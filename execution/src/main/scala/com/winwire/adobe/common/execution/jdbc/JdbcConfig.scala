package com.winwire.adobe.common.execution.jdbc

import scala.beans.BeanProperty

class JdbcSection {
  @BeanProperty var url: String = _
  @BeanProperty var driver: String = _
  @BeanProperty var user: String = _
  @BeanProperty var password: String = _
  @BeanProperty var fetchSize: String = _
}

trait JdbcConfig {
  @BeanProperty var jdbc: JdbcSection = _
}
