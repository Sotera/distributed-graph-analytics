package com.soteradefense.dga.graphx.wcc

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag


object WeaklyConnectionComponentsCore {
  def wcc[VD: ClassTag](graph: Graph[VD, Long]): (Graph[VertexId, Long]) = {
    graph.connectedComponents()
  }

}
