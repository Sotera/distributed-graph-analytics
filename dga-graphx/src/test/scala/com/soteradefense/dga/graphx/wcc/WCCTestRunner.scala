package com.soteradefense.dga.graphx.wcc

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class WCCTestRunner extends AbstractWCCRunner {

  override type S = Graph[Long, Long]

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = graph.asInstanceOf[Graph[Long, Long]]
}
