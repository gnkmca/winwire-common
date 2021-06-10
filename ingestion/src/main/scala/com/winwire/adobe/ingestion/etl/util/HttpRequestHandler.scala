package com.winwire.adobe.ingestion.etl.util

import com.winwire.adobe.ingestion.etl.config.{HttpConfig, ProxyConfig}
import okhttp3._
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpHeaders

import java.net.{InetSocketAddress, Proxy => JavaProxy}
import java.util.concurrent.TimeUnit

case class HttpResponse(code: Int, body: String)

object HttpRequestHandler {
  val JSON_REQUEST_TYPE = "application/json; charset=utf-8"

  def post(url: String, json: String, httpConfig: HttpConfig): HttpResponse = {
    val client = buildHttpClient(httpConfig)
    val jsonType = MediaType.get(JSON_REQUEST_TYPE)
    val body = RequestBody.create(json, jsonType)
    val request = new Request.Builder()
      .url(url)
      .post(body)
      .build

    execute(client, request)
  }

  private def execute(client: OkHttpClient, request: Request): HttpResponse = {
    val response = client.newCall(request).execute()
    val responseObject = HttpResponse(response.code(), response.body.string())
    if (response != null) response.close()

    responseObject
  }

  private def buildHttpClient(httpConfig: HttpConfig): OkHttpClient = {
    if (httpConfig != null) {
      val client = new OkHttpClient().newBuilder()
      if (StringUtils.isNotBlank(httpConfig.username))
        client.authenticator(new Authenticator {
          override def authenticate(route: Route, response: Response): Request = {
            import okhttp3.Credentials
            if (response.request.header(HttpHeaders.AUTHORIZATION) != null) return null

            val credential = Credentials.basic(httpConfig.username, httpConfig.password)
            response.request.newBuilder.header(HttpHeaders.AUTHORIZATION, credential).build()
          }
        })

      if (httpConfig.proxy != null) client.proxy(buildProxy(httpConfig.proxy))
      if (StringUtils.isNotBlank(httpConfig.connectTimeout)) client.connectTimeout(httpConfig.connectTimeout.toLong, TimeUnit.SECONDS)
      if (StringUtils.isNotBlank(httpConfig.readTimeout)) client.readTimeout(httpConfig.readTimeout.toLong, TimeUnit.SECONDS)
      if (StringUtils.isNotBlank(httpConfig.writeTimeout)) client.writeTimeout(httpConfig.writeTimeout.toLong, TimeUnit.SECONDS)

      client.build()
    } else {
      new OkHttpClient
    }
  }

  private def buildProxy(proxyConf: ProxyConfig): JavaProxy = {
    val address = new InetSocketAddress(proxyConf.proxyHost, proxyConf.proxyPort.toInt)
    new JavaProxy(JavaProxy.Type.HTTP, address)
  }

  def get(endpoint: String, parameters: Map[String, String], headers: Map[String, String], httpConfig: HttpConfig): HttpResponse = {
    val client = buildHttpClient(httpConfig)
    val httpUrl = buildUrlWithParams(endpoint, parameters)
    val requestBuilder = buildRequest(headers)

    val request = requestBuilder
      .url(httpUrl)
      .get
      .build

    execute(client, request)
  }

  private def buildUrlWithParams(url: String, parameters: Map[String, String]): HttpUrl = {
    val httpUrlBuilder = HttpUrl.parse(url).newBuilder()
    val builderWithParams = parameters.foldLeft(httpUrlBuilder) {
      case (httpUrlBuilder, (key, value)) => httpUrlBuilder.addQueryParameter(key, value)
    }

    builderWithParams.build()
  }

  private def buildRequest(headers: Map[String, String]): Request.Builder = {
    headers.foldLeft(new Request.Builder()) {
      case (requestBuilder, (key, value)) => requestBuilder.addHeader(key, value)
    }
  }

  def toJson(query: Any): String = query match {
    case map: Map[String, Any] => s"{${map.map(toJson(_)).mkString(",")}}"
    case tuple: (String, Any) => s""""${tuple._1}":${toJson(tuple._2)}"""
    case seq: Seq[Any] => s"""[${seq.map(toJson).mkString(",")}]"""
    case str: String => s""""$str""""
    case null => "null"
    case _ => query.toString
  }
}