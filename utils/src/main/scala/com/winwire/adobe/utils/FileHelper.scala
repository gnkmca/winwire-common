package com.winwire.adobe.utils

import com.winwire.adobe.utils.entities.AzureContext
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HDSFPath}

import java.io.{File, FileInputStream}

object FileHelper {

  def resolvePath(path: String) : String  = {
    FilenameUtils.separatorsToSystem(path)
  }

  def getFullHDFSPath(path: String): String = {
    val fs = FileSystem.get(new Configuration())
    val files = fs.listStatus(new HDSFPath(path))
    val fullpath = if (files.nonEmpty) {
      files.map(_.getPath.toString).filterNot(_.startsWith("_")).toList.head
    } else {
      ""
    }
    fullpath
  }

  def uploadFileToBlob(inputFile: String, outputAzureContext: AzureContext, outputBlobDir: String) = {
    val inStream = new FileInputStream(inputFile)
    val azureStorageHelperService: AzureStorageHelperService = AzureStorageHelper
    val fileName = new File(inputFile).getName
    val outputFile = s"$outputBlobDir$fileName"
    val azurePath = azureStorageHelperService.uploadToBlob(inStream, outputAzureContext, outputFile)
    azurePath
  }

  def hdfsToLocalFile(hdfsFile: String, localFile: String) = {

    val hadoopFS = FileSystem.get(new Configuration())
    hadoopFS.copyToLocalFile(new HDSFPath(hdfsFile), new HDSFPath(FileHelper.resolvePath(localFile)))
    localFile
  }

  def getFileDatePattern(datepattern: Option[String]) = {
    def getPattern(pattern: String) = {
      pattern.toLowerCase match {
        case "mmyyyyddhhmmss" => new java.text.SimpleDateFormat("MM-yyyy-dd-HHmmss")
        case "mmddyyyyhhmmss" => new java.text.SimpleDateFormat("MM-dd-yyyy-HHmmss")
        case "mmyyyydd" => new java.text.SimpleDateFormat("MM-yyyy-dd")
        case "yyyymmdd" => new java.text.SimpleDateFormat("yyyyMMdd")
        case "mmddyyyy" => new java.text.SimpleDateFormat("MMddyyyy")
      }
    }

    datepattern match {
      case Some(d) => getPattern(d)
      case None => new java.text.SimpleDateFormat("MM-dd-yyyy-HHmmss")
    }
  }

}
