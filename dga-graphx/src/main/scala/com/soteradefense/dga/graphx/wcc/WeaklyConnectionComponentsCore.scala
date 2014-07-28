package com.soteradefense.dga.graphx.wcc

import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.reflect.ClassTag


object WeaklyConnectionComponentsCore extends Logging{
  def wccGraphX[VD: ClassTag](graph: Graph[VD, Long]): (Graph[VertexId, Long]) = {
    graph.connectedComponents()
  }

  def wcc[VD: ClassTag](graph: Graph[VD, Long]): (Graph[VertexId, Long]) = {
    logInfo("Setting each vertex to the maximum neighbor value.")
    val vertexRDD = graph.mapReduceTriplets(e => Iterator((e.dstId, Math.max(e.dstId, e.srcId))), (a1: VertexId, a2: VertexId) => Math.max(a1, a2))
    logInfo("Creating the graph from the messages.")
    val componentGraph = graph.outerJoinVertices(vertexRDD) {
      (vid, vdata, highestValue) => highestValue.getOrElse(vid)
    }.cache()

    def sendMessage(edge: EdgeTriplet[VertexId, Long]) = {
      if (edge.srcAttr < edge.dstAttr)
        Iterator((edge.srcId, edge.dstAttr))
      else if (edge.dstAttr < edge.srcAttr)
        Iterator((edge.dstId, edge.srcAttr))
      else
        Iterator.empty
    }
    val initialValue = Long.MinValue
    val vertexDataSelector = (id: VertexId, vd: Long, attr: Long) => Math.max(vd, attr)
    val mergeMessage = (attr1: Long, attr2: Long) => Math.max(attr1, attr2)
    logInfo("Starting Pregel Operation")
    Pregel(componentGraph, initialValue, activeDirection = EdgeDirection.Either)(vertexDataSelector, sendMessage, mergeMessage)
  }
}
