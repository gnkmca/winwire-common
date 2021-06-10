
package com.winwire.adobe.ingestion.etl.config


import com.winwire.adobe.jdbc.ConnectionDetails

import scala.beans.BeanProperty

class MetastoreSection {
  @BeanProperty var db: String = _
  @BeanProperty var url: String = _
  @BeanProperty var driver: String = _
  @BeanProperty var username: String = _
  @BeanProperty var password: String = _
  @BeanProperty var processedFilesTable: String = _
  @BeanProperty var etlProcessesTable: String = _
  @BeanProperty var etlMetricsTable: String = _
  @BeanProperty var etlMetricsLogTable: String = _

  def connectionDetails: ConnectionDetails = ConnectionDetails(db, Map(
    s"$db.url" -> url,
    s"$db.driver" -> driver,
    s"$db.username" -> username,
    s"$db.password" -> password
  ))
}

trait MetastoreConfig {
  @BeanProperty var metastore: MetastoreSection = _
}

