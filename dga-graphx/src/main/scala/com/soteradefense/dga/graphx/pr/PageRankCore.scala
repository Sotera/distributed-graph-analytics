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


object PageRankCore extends Logging {

  private final val dampingFactor = 0.85f

  def prGraphX[VD: ClassTag](graph: Graph[VD, Long], delta: Double): (Graph[Double, Double]) = {
    graph.pageRank(delta)
  }

  def pr[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], delta: Double): (Graph[Double, Int]) = {
    logInfo("Starting the PageRank Algorithm")
    val numberOfVertices = graph.vertices.count()
    val initialVertexValue = 1.0 / numberOfVertices
    logInfo("Creating the PageRank Graph")
    val prGraph: Graph[(Double, Double), Int] = graph
      .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      .mapTriplets(e => 1 / e.srcAttr)
      .mapVertices((vid, vd) => (0.0, 0.0))
      .cache()

    def vertexProgram(id: VertexId, attr: (Double, Double), sumOfMessages: Double) = {
      val (previousPR, previousDelta) = attr
      val rank = ((1.0 - dampingFactor) / numberOfVertices) + (dampingFactor * sumOfMessages)
      var deltaVal = 0.0
      val rankDifference = Math.abs(rank - previousPR)
      deltaVal = rankDifference / previousPR
      (rank, deltaVal)
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    def sendMessage(edge: EdgeTriplet[(Double, Double), Int]) = {
      if (delta < edge.srcAttr._2) {
        Iterator((edge.dstId, edge.srcAttr._1 * edge.attr))
      }
      else {
        Iterator.empty
      }
    }

    logInfo("Starting Pregel Operation")
    Pregel(prGraph, initialVertexValue, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner).mapVertices((vid, attr) => attr._1)
  }

}
