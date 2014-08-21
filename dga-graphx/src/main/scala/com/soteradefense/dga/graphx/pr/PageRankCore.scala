/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.soteradefense.dga.graphx.pr

import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.reflect.ClassTag


class PageRankCore extends Logging with Serializable {

  private final val dampingFactor = 0.85f

  def runPageRankGraphX[VD: ClassTag](graph: Graph[VD, Long], delta: Double): (Graph[Double, Double]) = {
    graph.pageRank(delta)
  }

  def runPageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], delta: Double): (Graph[Double, Int]) = {
    logInfo("Starting the PageRank Algorithm")
    val numberOfVertices = graph.vertices.count()
    val initialVertexValue = 1.0 / numberOfVertices
    logInfo("Creating the PageRank Graph")
    val pageRankGraph: Graph[(Double, Double), Int] = graph
      .outerJoinVertices(graph.outDegrees) {
      (vertexId, pageRankData, degree) => degree.getOrElse(0)
    }
      .mapTriplets(triplet => 1 / triplet.srcAttr)
      .mapVertices((vertexId, vertexData) => (0.0, 0.0))
      .cache()

    def vertexProgram(vertexId: VertexId, pageRankData: (Double, Double), messageSum: Double) = {
      val (previousPageRank, previousDelta) = pageRankData
      val rank = ((1.0 - dampingFactor) / numberOfVertices) + (dampingFactor * messageSum)
      var delta = 0.0
      val rankDifference = Math.abs(rank - previousPageRank)
      delta = rankDifference / previousPageRank
      (rank, delta)
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    def sendMessage(triplet: EdgeTriplet[(Double, Double), Int]) = {
      val (pageRank, pageRankDifference) = triplet.srcAttr
      if (delta < pageRankDifference) {
        Iterator((triplet.dstId, pageRank * triplet.attr))
      }
      else {
        Iterator.empty
      }
    }

    logInfo("Starting Pregel Operation")
    Pregel(pageRankGraph, initialVertexValue, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
      .mapVertices((vertexId, pageRankData) => {
      val (pageRank, delta) = pageRankData
      pageRank
    })
  }

}
