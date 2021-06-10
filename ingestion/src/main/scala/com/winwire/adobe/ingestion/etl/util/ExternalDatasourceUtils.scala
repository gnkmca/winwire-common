package com.winwire.adobe.ingestion.etl.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

object ExternalDatasourceUtils {

  def getFileSystem(path: String, conf: Configuration): FileSystem = FileSystem.get(new URI(path), conf)

  def countFiles(eventsPath: String, fs: FileSystem): Int = fs.listStatus(new Path(eventsPath)).length

  def resolveSubdirs(fs: FileSystem, path: String): Seq[String] = {
    val files = fs.listStatus(new Path(path))
    val subDirs = files.filter(s => s.isDirectory)
    if (subDirs.length == files.length) {
      subDirs.map(s => resolveSubdirs(fs, s.getPath.toString)).flatMap(_.toSeq)
    } else {
      Seq(path)
    }
  }
}
