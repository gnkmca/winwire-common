/*package com.winwire.adobe.ingestion.etl.config

import com.winwire.adobe.sftp.SFTPConfig
import com.winwire.adobe.ingestion.etl.datawriter.DataFileWriterConfig
import com.winwire.adobe.ingestion.etl.sql.TableConfig

import scala.beans.BeanProperty

class OutboundTableConfig extends TableConfig with DataFileWriterConfig {
  @BeanProperty var rawDataDbfsPath: String = _
}

class PgpStep {
  @BeanProperty var recipient: String = _
  @BeanProperty var pgp: PGPConfig = _
  @BeanProperty var armor: Boolean = _
}

class CustomOutboundSFTPConfig extends SFTPConfig {
  @BeanProperty var outputDateTimeFormat: String = _
  @BeanProperty var outputFilenameFormat: String = _
  @BeanProperty var rewrite: Boolean = true
  @BeanProperty var useTunnel: Boolean = false
}

class OutboundApplicationConfig[T <: OutboundTableConfig] extends AppConfig {

  @BeanProperty var table: T = _
  @BeanProperty var withPgp: PgpStep = _
  @BeanProperty var sftp: CustomOutboundSFTPConfig = _
}


 */