package com.winwire.adobe.utils.entities

import scala.beans.BeanProperty

class AzureContext {
  @BeanProperty var connectionString: String = _
  @BeanProperty var container: String = _
  @BeanProperty var directory: String = _
}
