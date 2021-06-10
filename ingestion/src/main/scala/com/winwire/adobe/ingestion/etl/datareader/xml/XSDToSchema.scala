package com.winwire.adobe.ingestion.etl.datareader.xml

import com.sun.xml.xsom._
import com.sun.xml.xsom.parser.XSOMParser
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.types._

import java.nio.charset.Charset
import javax.xml.parsers.SAXParserFactory
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.xml.SAXException

sealed case class SparkSchemaNode(name: String, dataType: DataType, maxOccurs: BigInt)

object XSDToSchema {

  val ATTRIBUTE_PREFIX = "_"
  val XmlToSparkTypes: Map[String, DataType] =
    Map("string" -> DataTypes.StringType,
      "boolean" -> DataTypes.BooleanType,
      "decimal" -> DataTypes.DoubleType,
      "float" -> DataTypes.FloatType,
      "double" -> DataTypes.DoubleType,
      "duration" -> DataTypes.StringType,
      "dateTime" -> DataTypes.StringType,
      "time" -> DataTypes.StringType,
      "date" -> DataTypes.StringType,
      "gYearMonth" -> DataTypes.StringType,
      "gYear" -> DataTypes.StringType,
      "gMonthDay" -> DataTypes.StringType,
      "gDay" -> DataTypes.StringType,
      "gMonth" -> DataTypes.StringType,
      "hexBinary" -> DataTypes.BinaryType,
      "base64Binary" -> DataTypes.BinaryType,
      "anyURI" -> DataTypes.StringType
    )

  def xsdToSparkSchema(schema: String): StructType = {

    val parser = new XSOMParser(SAXParserFactory.newInstance())
    //method parse() doesn't throw any exception if file doesn't exist
    parser.parse(IOUtils.toInputStream(schema, Charset.defaultCharset()))

    val myTree: Tree[SparkSchemaNode] = new Tree[SparkSchemaNode]
    val schemaSet: XSSchemaSet = parser.getResult
    var arr: Array[StructField] = Array.empty

    for (xsSchema <- schemaSet.getSchemas.asScala) {
      val elems: mutable.Map[String, XSElementDecl] = xsSchema.getElementDecls.asScala
      val attrs: mutable.Map[String, XSAttributeDecl] = xsSchema.getAttributeDecls.asScala

      myTree.setRoot(SparkSchemaNode("root", null, 1))

      for (elem <- elems) {
        val e: XSElementDecl = elem._2
        buildNode(e, 1, myTree.getRoot, ATTRIBUTE_PREFIX)
      }

      for (attr <- attrs) {
        val a: XSAttributeDecl = attr._2
        myTree.getRoot.addChild(SparkSchemaNode(ATTRIBUTE_PREFIX + a.getName, matchingSparkType(a.getType), 1))
      }

      for (child <- myTree.getRoot.children) {
        val z: StructType = buildSparkSchemaFromNode(new StructType, child)
        arr = arr ++ z.fields
      }
    }
    StructType(arr)
  }

  def buildSparkSchemaFromNode(schema: StructType, currNode: Tree[SparkSchemaNode]#Node): StructType = {
    val sparkType = {
      if (currNode.data.dataType == null) {
        var structType = new StructType()
        for (c <- currNode.children) {
          structType = buildSparkSchemaFromNode(structType, c)
        }

        if (currNode.data.maxOccurs == -1 /*unbounded*/ ) {
          DataTypes.createArrayType(structType)
        } else {
          structType
        }
      } else {
        currNode.data.dataType
      }
    }
    schema.add(currNode.data.name, sparkType)
  }

  def buildNode(elem: XSElementDecl, maxOccurs: BigInt, parentNode: Tree[SparkSchemaNode]#Node, ATTRIBUTE_PREFIX: String): Tree[SparkSchemaNode]#Node = {
    val currentNode = parentNode.addChild(SparkSchemaNode(elem.getName, matchingSparkType(elem.getType), maxOccurs))

    if (elem.getType.isSimpleType) {
      currentNode.parent
    }
    else if (elem.getType.isComplexType) {
      val particle = elem.getType.asComplexType().getContentType.asParticle()
      if (particle != null) {
        val children = particle.getTerm.asModelGroup().getChildren
        for (child <- children) {
          buildNode(child.getTerm.asElementDecl(), child.getMaxOccurs, currentNode, ATTRIBUTE_PREFIX)
        }
      }
      val attrs = elem.getType.asComplexType().getAttributeUses.asScala
      for (attr <- attrs) {
        currentNode.addChild(SparkSchemaNode(ATTRIBUTE_PREFIX + attr.getDecl.getName, matchingSparkType(attr.getDecl.getType), 1))
      }
      currentNode.parent
    }
    else {
      throw new SAXException("XSD element is not recognized as either simple or complex type!")
    }
  }

  def matchingSparkType(xmlType: XSType): DataType = {
    if (xmlType.getName == "anyType") {
      null
    }
    else if (XmlToSparkTypes.contains(xmlType.getName)) {
      XmlToSparkTypes(xmlType.getName)
    }
    else {
      matchingSparkType(xmlType.getBaseType)
    }
  }
}