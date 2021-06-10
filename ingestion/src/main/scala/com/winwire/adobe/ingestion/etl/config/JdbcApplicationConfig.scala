package com.winwire.adobe.ingestion.etl.config

import com.winwire.adobe.execution.jdbc.JdbcConfig
import com.winwire.adobe.ingestion.etl.sql.TableConfig

import scala.beans.BeanProperty

/*trait CustomJdbcConfig extends JdbcConfig {

}
*/

class JdbcTableConfig extends TableConfig {
  @BeanProperty var useNativeJdbc: Boolean = false
}

class JdbcApplicationConfig[T <: JdbcTableConfig] extends AppConfig with JdbcConfig {
  @BeanProperty var table: T = _
}