package com.winwire.adobe.ingestion.etl.util

import scala.beans.BeanProperty

class KeyValue {
  @BeanProperty var key: String = _
  @BeanProperty var value: String = _
}