package com.winwire.adobe.applications
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import com.winwire.adobe.execution.kafka.KafkaConfig

import scala.beans.BeanProperty

class SnapshottingConfig extends KafkaConfig {
  @BeanProperty var jobName: String = _
  @BeanProperty var sourceTopic: String = _
  @BeanProperty var sinkTable: String = _
  @BeanProperty var checkpointLocation: String = _
}
