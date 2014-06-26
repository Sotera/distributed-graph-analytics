package com.soteradefense.dga.graphx.io.formats

import com.esotericsoftware.kryo.Serializer
import com.twitter.chill._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD


case class EdgeInputFormat(var inputFile: String, var delimiter: String) extends Serializable  {
  def getEdgeRDD(sc: SparkContext, typeConversionMethod: String => Long = _.toLong): RDD[Edge[Long]] = {
    sc.textFile(inputFile).map(row => {
      val tokens = row.split(delimiter).map(_.trim())
      tokens.length match {
        case 2 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), 1L)
        case 3 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), tokens(2).toLong)
        case _ => throw new IllegalArgumentException("invalid input line: " + row)
      }
    })
  }
}

class EdgeInputFormatSerializer extends Serializer[EdgeInputFormat] {
  override def write(kryo: Kryo, out: Output, obj: EdgeInputFormat): Unit = {
    kryo.writeObject(out, obj.inputFile)
    kryo.writeObject(out, obj.delimiter)
  }

  override def read(kryo: Kryo, in: Input, cls: Class[EdgeInputFormat]): EdgeInputFormat = {
    new EdgeInputFormat(kryo.readObject(in, classOf[String]), kryo.readObject(in, classOf[String]))
  }
}