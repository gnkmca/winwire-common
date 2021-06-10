package com.winwire.adobe.execution.jdbc

import com.winwire.adobe.execution.BaseTest
import com.winwire.adobe.execution.spark.{SparkApplication, SparkSessionProvider}
import com.winwire.adobe.utils.KeyValue
import org.apache.spark.sql._
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach

class JdbcTest extends BaseTest
  with MockitoSugar
  with BeforeAndAfterEach {

  behavior of classOf[Jdbc].getSimpleName

  private var sparkMock: SparkSession = _
  private var dfReader: DataFrameReader = _
  private var jdbcDf: DataFrame = _

  override def beforeEach(): Unit = {
    sparkMock = mock[SparkSession]
    dfReader = mock[DataFrameReader]
    jdbcDf = mock[DataFrame]

    when(sparkMock.read).thenReturn(dfReader)

    when(dfReader.format(any[String])).thenReturn(dfReader)
    when(dfReader.option(any[String], any[String])).thenReturn(dfReader)
    when(dfReader.options(any[Map[String, String]])).thenReturn(dfReader)
    when(dfReader.load()).thenReturn(jdbcDf)
  }

  trait MockSparkSessionProvider extends SparkSessionProvider {
    override protected def getOrCreateSparkSession(appName: String): SparkSession = sparkMock
  }


  class TestConfig extends JdbcConfig

  class MockApp extends SparkApplication[TestConfig] with DefaultJdbc {

    override def name: String = "jdbc"

    override def script(): Unit = {}

    override def config: TestConfig = new TestConfig {
      jdbc = new JdbcSection {
        url = "url"
        driver = "driver"
        user = "user"
        password = "password"
        fetchSize = "fetchSize"
        extraOptions = Array(new KeyValue{
          key = "key"
          value = "value"
        })
      }
    }
  }

  it should "should set proper configuration to read data" in {
    val app = new MockApp with MockSparkSessionProvider

    app.readFromJdbc("jdbc_statement")

    verify(sparkMock, times(1)).read
    verify(dfReader, times(1)).format("jdbc")
    verify(dfReader, times(1)).option("url", "url")
    verify(dfReader, times(1)).option("driver", "driver")
    verify(dfReader, times(1)).option("user", "user")
    verify(dfReader, times(1)).option("password", "password")
    verify(dfReader, times(1)).option("dbtable", "jdbc_statement")
    verify(dfReader, times(1)).option("fetchSize", "fetchSize")
    verify(dfReader, times(1)).options(Map("key" -> "value"))
    verify(dfReader, times(1)).load()
  }

}


/*package com.winwire.adobe.execution.jdbc

import com.winwire.adobe.execution.BaseTest
import com.winwire.adobe.execution.spark.{SparkApplication, SparkSessionProvider}
import org.apache.spark.sql._
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach

class JdbcTest extends BaseTest
  with MockitoSugar
  with BeforeAndAfterEach {

  behavior of classOf[Jdbc].getSimpleName

  private var sparkMock: SparkSession = _
  private var dfReader: DataFrameReader = _
  private var jdbcDf: DataFrame = _

  override def beforeEach(): Unit = {
    sparkMock = mock[SparkSession]
    dfReader = mock[DataFrameReader]
    jdbcDf = mock[DataFrame]

    when(sparkMock.read).thenReturn(dfReader)

    when(dfReader.format(any[String])).thenReturn(dfReader)
    when(dfReader.option(any[String], any[String])).thenReturn(dfReader)
    when(dfReader.load()).thenReturn(jdbcDf)
  }

  trait MockSparkSessionProvider extends SparkSessionProvider {
    override protected def getOrCreateSparkSession(appName: String): SparkSession = sparkMock
  }


  class TestConfig extends JdbcConfig

  class MockApp extends SparkApplication[TestConfig] with DefaultJdbc {

    override def name: String = "jdbc"

    override def script(): Unit = {}

    override def config: TestConfig = new TestConfig {
      jdbc = new JdbcSection {
        url = "url"
        driver = "driver"
        user = "user"
        password = "password"
        fetchSize = "fetchSize"
      }
    }
  }

  it should "should set proper configuration to read data" in {
    val app = new MockApp with MockSparkSessionProvider

    app.readFromJdbc("jdbc_statement")

    verify(sparkMock, times(1)).read
    verify(dfReader, times(1)).format("jdbc")
    verify(dfReader, times(1)).option("url", "url")
    verify(dfReader, times(1)).option("driver", "driver")
    verify(dfReader, times(1)).option("user", "user")
    verify(dfReader, times(1)).option("password", "password")
    verify(dfReader, times(1)).option("dbtable", "jdbc_statement")
    verify(dfReader, times(1)).option("fetchSize", "fetchSize")
    verify(dfReader, times(1)).load()
  }
}
*/