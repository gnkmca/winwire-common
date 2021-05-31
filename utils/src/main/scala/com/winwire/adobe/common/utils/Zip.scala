package com.winwire.adobe.common.utils

import better.files._
import org.apache.commons.io.FilenameUtils

object Zip {
  type JavaFile = java.io.File

  def unzipAll(directory: String, destinationPath: String): Unit = {
    val dir = new JavaFile(directory)

    val files = if (dir.exists && dir.isDirectory) {
      dir.listFiles
        .filter(f => f.isFile && FilenameUtils.getExtension(f.getName) == "zip")
        .toList
    } else List()

    files.foreach(f => unzip(f.getAbsolutePath, destinationPath))
  }

  def unzip(file: String, destinationPath: String): Unit = {
    val zipFile: File = file"$file"
    val destination = file"$destinationPath"
    zipFile.unzipTo(destination)
  }
}
