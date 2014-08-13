package com.soteradefense.dga.graphx.hbse

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class HBSETestRunner extends AbstractHBSERunner {

  override type H = (RDD[(Long, Double)], Graph[VertexData, Long])
  override type S = Graph[VertexData, Long]

  override def save[ED: ClassTag](betweennessSet: RDD[(Long, Double)], graph: Graph[VertexData, ED]): H = (betweennessSet, save(graph))

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = graph.asInstanceOf[Graph[VertexData, Long]]

}
