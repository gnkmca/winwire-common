/*
package com.winwire.adobe.ingestion.etl.config

import com.winwire.adobe.sftp.SFTPConfig
import com.winwire.adobe.ingestion.etl.datareader.DataFileReaderConfig
import com.winwire.adobe.ingestion.etl.sql.TableConfig

import scala.beans.BeanProperty

class PGPConfig {
  @BeanProperty var passphrase: String = _
  @BeanProperty var secureKeyRingPath: String = _
  @BeanProperty var publicKeyRingPath: String = _
}

class SftpTableConfig extends TableConfig with DataFileReaderConfig

class CustomSFTPConfig extends SFTPConfig {
  @BeanProperty var fileNameRegexPattern: String = _
  @BeanProperty var localDataPath: String = _
  @BeanProperty var filesPerRun: Int = -1
}

class SftpApplicationConfig[T <: SftpTableConfig] extends AppConfig {
  @BeanProperty var sftp: CustomSFTPConfig = _
  @BeanProperty var pgp: PGPConfig = _
  @BeanProperty var unzip: Boolean = false
  @BeanProperty var table: T = _
}

*/