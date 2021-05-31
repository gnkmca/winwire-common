package com.winwire.adobe.common.execution

trait Job[C] {

  def config: C

  def name: String

  def script(): Unit
}
