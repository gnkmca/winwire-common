/*package com.winwire.adobe.common.config.azure

import com.azure.identity.{ClientSecretCredential, ClientSecretCredentialBuilder}
import com.azure.security.keyvault.secrets.{SecretClient, SecretClientBuilder}
import com.winwire.adobe.common.config.SecretsStorage
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.util.{Failure, Success, Try}

private[config] class KvSecretsStorage(vaultConfig: => AzureKVSection) extends SecretsStorage with LazyLogging {

  lazy val credentials: ClientSecretCredential = new ClientSecretCredentialBuilder()
    .tenantId(vaultConfig.tenantId)
    .clientId(vaultConfig.clientId)
    .clientSecret(vaultConfig.clientSecret)
    .build()

  lazy val client: SecretClient = new SecretClientBuilder()
    .vaultUrl(vaultConfig.url)
    .credential(credentials)
    .buildClient()

  override def get(name: String): Option[String] = {
    val secret = Try(client.getSecret(name))

    secret match {
      case Success(value) => Some(value.getValue)
      case Failure(e) =>
        logger.error(s"Failed to load secret $name from Azure KV", e)
        None
    }
  }
}
*/