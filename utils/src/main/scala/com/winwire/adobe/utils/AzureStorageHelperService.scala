package com.winwire.adobe.utils

import com.microsoft.azure.storage.blob.ListBlobItem
import com.winwire.adobe.utils.entities.AzureContext
import org.apache.hadoop.fs.FSDataInputStream

import java.io.FileInputStream

/**
  * Created by Naveen Gajja
  *
  * Azure Storage helper methods
  *
  */
trait AzureStorageHelperService {

  def getBlobs(context: AzureContext, directorySuffix: String): Iterable[ListBlobItem]

  def uploadToBlob(inputStream: FSDataInputStream, azureContext: AzureContext, azureOutputPath: String):String

  def uploadToBlob(inputStream: FileInputStream, azureContext: AzureContext, azureOutputPath: String): String

  def filesExists(path: String, context: AzureContext): Boolean
}
