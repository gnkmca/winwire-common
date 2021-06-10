package com.winwire.adobe.utils.entities

object Entities {

  case class AzureContext(connectionString: String,
                          storageAccount: String,
                          container: String,
                          directory: String)

}
