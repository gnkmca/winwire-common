package com.winwire.adobe.ingestion.etl.util

import com.winwire.adobe.ingestion.etl.BaseTest
import org.mockito.MockitoSugar

class ParamsTest extends BaseTest
  with MockitoSugar {

  val query = "${db}.${table}.${column}"

  "Params" should "insert all values into sql" in {
    val params = Map(
      "db" -> "db", "table" -> "table", "column" -> "column"
    )
    val newQuery = Params.insertParams(query, params)
    newQuery shouldBe "db.table.column"
  }

  "Params" should "skip nulls" in {
    val params = Map(
      "db" -> "db", "table" -> "table", "column" -> "column",
      "value" -> null
    )
    val newQuery = Params.insertParams(query, params)
    newQuery shouldBe "db.table.column"
  }

  "Params" should "not insert extra values" in {
    val params = Map(
      "db" -> "db", "table" -> "table", "column" -> "column",
      "value" -> "value"
    )
    val newQuery = Params.insertParams(query, params)
    newQuery shouldBe "db.table.column"
  }

}
