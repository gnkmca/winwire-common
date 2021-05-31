/*package com.winwire.adobe.common.config

import com.winwire.adobe.common.config.azure.{AzureKVConfig, KvSecretsStorage}

trait SecretsStorageProvider {
  def secretsStorage[A](config: => A): SecretsStorage = {
    val kvConfig = config.asInstanceOf[AzureKVConfig]
    new KvSecretsStorage(kvConfig.keyVault)
  }
}
*/