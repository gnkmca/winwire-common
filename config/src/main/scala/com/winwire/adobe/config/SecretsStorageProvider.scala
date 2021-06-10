package com.winwire.adobe.config
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import com.winwire.adobe.config.azure.{AzureKVConfig, KvSecretsStorage}

trait SecretsStorageProvider {
  def secretsStorage[A](config: => A): SecretsStorage = {
    val kvConfig = config.asInstanceOf[AzureKVConfig]
    new KvSecretsStorage(kvConfig.keyVault)
  }
}
