package com.winwire.adobe.ingestion.etl.datareader.xml

class Tree[A] {

  private var root: Node = _

  def getRoot: Node = root

  def setRoot(data: A): Unit = {
    root = new Node(data)
  }

  def isEmpty: Boolean = root == null

  def traverse(visit: A => Unit): Unit = {
    def recur(node: Node): Unit = {
      for (child <- node.children) recur(child)
      visit(node.data)
    }

    recur(root)
  }

  def traverseWhile(condition: A => Boolean): Option[Node] = {
    def recur(node: Node): Option[Node] = {
      if (condition(node.data)) {
        Some(node)
      }
      else {
        for (child <- node.children) recur(child)
        None
      }
    }

    recur(root)
  }

  class Node(val data: A, var parent: Node = null, var children: Set[Node] = Set.empty) {
    override def clone(): Node = {
      new Node(this.data, this.parent, this.children)
    }

    def addChild(data: A): Node = {
      val node = new Node(data, this)
      children = children + node
      node
    }
  }

}
