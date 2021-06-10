package com.winwire.adobe.ingestion.etl.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.util.UUID
import scala.util.hashing.MurmurHash3

trait UDFUtils {

  private lazy val uuidUDF: UserDefinedFunction =
    udf((colValue: String) => {
      UUID.nameUUIDFromBytes(colValue.getBytes).toString
    })

  private lazy val customerAddressHash: UserDefinedFunction =
    udf((address1: String, address2: String, address3: String, city: String, state: String, postalCode: String) => {
      val combinedAddress = s"$address1$address2$address3$city$state$postalCode"
      MurmurHash3.stringHash(combinedAddress, MurmurHash3.stringSeed)
    })

  protected def getUDFs: Map[String, UserDefinedFunction] = {
    Map(
      "uuidUDF" -> uuidUDF,
      "customerAddressHashUDF" -> customerAddressHash
    )
  }

}
