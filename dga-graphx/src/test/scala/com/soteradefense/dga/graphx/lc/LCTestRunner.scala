package com.soteradefense.dga.graphx.lc

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class LCTestRunner extends AbstractLCRunner {

  override type S = Graph[Long, Long]

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = graph.asInstanceOf[Graph[Long, Long]]


}
