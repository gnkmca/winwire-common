package com.winwire.adobe.execution
/**
  * Created by Naveen Gajja on 06/01/2021.
  */
trait Job[C] {

  def config: C

  def name: String

  def script(): Unit
}
