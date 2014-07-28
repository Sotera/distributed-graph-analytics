package com.soteradefense.dga.graphx.pr

import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.reflect.ClassTag


object PageRankCore extends Logging {

  private final val dampingFactor = 0.85f

  def prGraphX[VD: ClassTag](graph: Graph[VD, Long], delta: Double): (Graph[Double, Double]) = {
    graph.pageRank(delta)
  }

  def pr[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], delta: Double): (Graph[Double, Long]) = {
    logInfo("Starting the PageRank Algorithm")
    val numberOfVertices = graph.vertices.count()
    val initialVertexValue = 1.0 / numberOfVertices
    logInfo("Creating the PageRank Graph")
    val prGraph: Graph[(Double, Double), Long] = graph
      .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0).toLong
    }
      .mapTriplets(e => e.srcAttr)
      .mapVertices((vid, vd) => (0.0, 0.0))
      .cache()

    def vertexProgram(id: VertexId, attr: (Double, Double), sumOfMessages: Double) = {
      val (previousPR, previousDelta) = attr
      val rank = ((1.0 - dampingFactor) / numberOfVertices) + (dampingFactor * sumOfMessages)
      var deltaVal = 0.0
      val rankDifference = rank - previousPR
      deltaVal = if (previousPR != 0.0) Math.abs(rankDifference) / previousPR else Math.abs(rankDifference)
      (rank, deltaVal)
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    def sendMessage(edge: EdgeTriplet[(Double, Double), Long]) = {
      if (edge.srcAttr._2 > delta) {
        Iterator((edge.dstId, edge.srcAttr._1 / edge.attr))
      }
      else {
        logInfo(s"Delta met, not sending message to ${edge.dstId}")
        Iterator.empty
      }
    }

    logInfo("Starting Pregel Operation")
    Pregel(prGraph, initialVertexValue, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner).mapVertices((vid, attr) => attr._1)
  }

}
