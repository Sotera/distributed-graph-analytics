package com.soteradefense.dga.graphx.lc

import org.apache.spark.Logging
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag


object LeafCompressionCore extends Logging {
  def lc[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Long, Long] = {
    val previousGraph = graph.outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0).toLong}.mapTriplets(e => e.srcAttr).cache()
    val filteredGraph = previousGraph.subgraph((e: EdgeTriplet[Long, Long]) => e.attr != 1 && e.attr != 0, (vid: VertexId, vdata: Long) => vdata != 0 && vdata != 1)
    val graphChanged = previousGraph.vertices.count() != filteredGraph.vertices.count()
    if (graphChanged)
      LeafCompressionCore.lc(filteredGraph)
    else
      filteredGraph
  }
}
