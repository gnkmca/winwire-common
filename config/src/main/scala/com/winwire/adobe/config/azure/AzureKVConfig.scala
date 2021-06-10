
package com.winwire.adobe.config.azure
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
import com.winwire.adobe.config.ContainsSecrets

import scala.beans.BeanProperty

class AzureKVSection {
  @BeanProperty var url: String = _
  @BeanProperty var tenantId: String = _
  @BeanProperty var clientId: String = _
  @BeanProperty var clientSecret: String = _
}

trait AzureKVConfig extends ContainsSecrets {
  @BeanProperty var keyVault: AzureKVSection = _
}
