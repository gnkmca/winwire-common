package com.winwire.adobe.utils

/**
 * Created by Naveen Gajja on 06/09/2021.
 */

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, SaveMode}

object DataFrameHelper {

  def createSingleFile(outbound: DataFrame, hdfsRawPath: String, isHeader: Boolean, isQuoteAll: Boolean, delimiter: String, isEscapeQuotes: Boolean, emptyValue: String): String = {
    val hdfsPath = FileHelper.resolvePath(hdfsRawPath)
    // create a single file for outbound
    outbound.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", isHeader)
      .option("delimiter", delimiter)
      .option("escapeQuotes", isEscapeQuotes)
      .option("quoteAll", isQuoteAll)
      .option("emptyValue", emptyValue)
      .format("com.databricks.spark.csv")
      .csv(hdfsPath)
    //logInfo("********************* outputHdfsPath ***************" + hdfsPath)
    deleteExtraFiles(hdfsPath)
    val tempPath = FileHelper.getFullHDFSPath(hdfsPath)
    tempPath
  }

  def deleteExtraFiles(sourcePath: String): Unit = {
    val fs = FileSystem.get(new Configuration())
    fs.listStatus(new Path(s"$sourcePath"))
      .filter(file => file.isFile && FilenameUtils.getName(file.getPath.toString).startsWith("_"))
      .foreach(file => fs.delete(file.getPath, false))
  }

  def replaceNewLineCharacter(dataFrame: DataFrame): DataFrame = {
    dataFrame.columns.foldLeft(dataFrame) {
      (df, colName) => df.withColumn(colName, regexp_replace(col(colName), "(\r\n|\n|\r)", " "))
    }
  }

  def replaceDoubleQuote(dataFrame: DataFrame): DataFrame = {
    dataFrame.columns.foldLeft(dataFrame) {
      (df, colName) => df.withColumn(colName, regexp_replace(col(colName), "\"", ""))
    }
  }


}
