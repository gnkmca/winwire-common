package com.winwire.adobe.utils

import java.io.FileInputStream
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.{CloudBlobClient, CloudBlobContainer, CloudBlobDirectory, CloudBlockBlob, ListBlobItem}
import com.winwire.adobe.utils.entities.AzureContext
import org.apache.hadoop.fs.FSDataInputStream

import scala.collection.JavaConversions._

object AzureStorageHelper extends AzureStorageHelperService {

  val DATE_FORMAT = new java.text.SimpleDateFormat("yyyyMMdd")
  val TIME_FORMAT = new java.text.SimpleDateFormat("HHmmss")

  def getBlobClient(connection: String) = {
    val storageAccount: CloudStorageAccount = CloudStorageAccount.parse(connection)
    storageAccount.createCloudBlobClient()
  }

  def getBlobContainer(blobClient: CloudBlobClient, containername: String) = {
    val container: CloudBlobContainer = blobClient.getContainerReference(containername)
    container
  }

  def getBlobDirectory(container: CloudBlobContainer, directory: String): CloudBlobDirectory = {
    container.getDirectoryReference(directory)

  }

  def getBlobReference(container: CloudBlobContainer, filename: String): CloudBlockBlob = {
    container.getBlockBlobReference(filename)

  }


  def getContainer(context: AzureContext) = {
    val client = getBlobClient(context.connectionString)
    client.getContainerReference(context.container)
  }

  def getDirectory(context: AzureContext) = {
    val container = getContainer(context)
    container.getDirectoryReference(context.directory)
  }

  def getBlobs(context: AzureContext) = {
    val directory = getDirectory(context)
    directory.listBlobs()
  }

  /**
    * Get Blobs under a directory with directory prefix
    *
    * @param context
    * @param directorySuffix
    * @return
    */
  override def getBlobs(context: AzureContext, directorySuffix: String) = {
    val directory = getDirectory(context, directorySuffix)
    directory.listBlobs()
  }

  /**
    * Get Blob directory reference with directoryPrefix
    *
    * @param context
    * @param directorySuffix
    * @return
    */
  def getDirectory(context: AzureContext, directorySuffix: String) = {
    val container = getContainer(context)
    container.getDirectoryReference(context.directory + directorySuffix)
  }

  /**
    * resolves and builds the blob path from cloud block blob
    * @param blob source blob
    * @return a formatted blob path
    */
  def resolveBlobPath(blob: CloudBlockBlob) = {
    s"wasbs://${blob.getContainer.getName}@${blob.getUri.getHost}/${blob.getName}"
  }

  /**
    * verfies if blob or files exists in given path.
    * the path could be in azure storage or a local/hdfs path
    * @param path path of file(s)
    * @param context azure context, required if path points to azure blob storage
    * @return true if file exists else false
    */
  override def filesExists(path: String, context: AzureContext) = {
    val container = getContainer(context)
    val result = try {
      val directory = container.getDirectoryReference(path)
      val res = directory.listBlobs().toList.length == 0
      !res
    } catch {
      case ex: Exception => {
        false
      }
    }
    result
  }

  /**
    * resolves and extracts the file name with folder
    * @param path
    * @return
    */
  def resolveBlobPath(path: String) : String = {
    if (path.contains("blob.core.windows.net/")) {
      path.split("blob.core.windows.net/")(1)
    }
    else {
      path
    }
  }

  /**
    * Upload input stream to Azure Blob
    *
    * @param inputStream
    * @param azureContext
    * @param azureOutputPath
    */
  override def uploadToBlob(inputStream: FSDataInputStream, azureContext: AzureContext, azureOutputPath: String): String = {
    val targetClient = getBlobClient(azureContext.connectionString)
    val container = getBlobContainer(targetClient, azureContext.container)
    val blob = container.getBlockBlobReference(azureOutputPath)
    blob.upload(inputStream, -1)
    //Close files
    inputStream.close()
    resolveBlobPath(blob)
  }

  override def uploadToBlob(inputStream: FileInputStream, azureContext: AzureContext, azureOutputPath: String): String = {
    val targetClient = getBlobClient(azureContext.connectionString)
    val container = getBlobContainer(targetClient, azureContext.container)
    val blob = container.getBlockBlobReference(azureOutputPath)
    blob.upload(inputStream, -1)
    //Close files
    inputStream.close()
    resolveBlobPath(blob)
  }
}
