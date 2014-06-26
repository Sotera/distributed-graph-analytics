package com.soteradefense.dga.graphx.io.formats

import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD


class EdgeInputFormat(data: RDD[String], delimiter: String) extends Serializable {
  def getEdgeRDD(typeConversionMethod: String => Long = _.toLong): RDD[Edge[Long]] = {
    data.map(row => {
      val tokens = row.split(delimiter).map(_.trim())
      tokens.length match {
        case 2 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), 1L)
        case 3 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), tokens(2).toLong)
        case _ => throw new IllegalArgumentException("invalid input line: " + row)
      }
    })
  }
}
