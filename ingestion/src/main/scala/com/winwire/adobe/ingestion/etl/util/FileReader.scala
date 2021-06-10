package com.winwire.adobe.ingestion.etl.util

import java.io.{File, FileNotFoundException, InputStream}
import scala.io.Source

object FileReader {

  def read(dir: String, filename: String): String = {
    read(s"$dir/$filename")
  }

  def read(path: String): String = {
    val finalPath = s"/$path"

    val stream: InputStream = getClass.getResourceAsStream(finalPath)
    val bufferedSource = Source.fromInputStream(stream)
    val data = bufferedSource.getLines.mkString("\n")
    bufferedSource.close()
    data
  }

  def listDirs(path: String): List[String] = {
    val resourcePath = getClass.getResource(s"/$path")
    val folder = new File(resourcePath.getPath)
    if (folder.exists && folder.isDirectory) {
      folder.listFiles.map(_.getName)
        .toList
    } else {
      throw new FileNotFoundException(s"$path doesn't exist")
    }
  }
}
