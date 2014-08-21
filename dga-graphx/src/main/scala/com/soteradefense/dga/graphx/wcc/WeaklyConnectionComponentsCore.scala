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
package com.soteradefense.dga.graphx.wcc

import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.reflect.ClassTag


class WeaklyConnectionComponentsCore extends Logging with Serializable {
  def runWeaklyConnectedComponentsGraphX[VD: ClassTag](graph: Graph[VD, Long]): (Graph[VertexId, Long]) = {
    graph.connectedComponents()
  }

  def runWeaklyConnectedComponents[VD: ClassTag](graph: Graph[VD, Long]): (Graph[VertexId, Long]) = {
    logInfo("Setting each vertex to the maximum neighbor value.")
    val initialComponentCalculation: VertexRDD[VertexId] = graph.mapReduceTriplets(triplet => {
      Iterator((triplet.dstId, Math.max(triplet.dstId, triplet.srcId)))
    }, (vertexId1: VertexId, vertexId2: VertexId) => {
      Math.max(vertexId1, vertexId2)
    })
    logInfo("Creating the graph from the messages.")
    val componentGraph = graph.outerJoinVertices(initialComponentCalculation) {
      (vertexId, vertexData, initialComponentId) => initialComponentId.getOrElse(vertexId)
    }.cache()

    def sendComponentIdUpdate(triplet: EdgeTriplet[VertexId, Long]) = {
      if (triplet.srcAttr < triplet.dstAttr)
        Iterator((triplet.srcId, triplet.dstAttr))
      else if (triplet.dstAttr < triplet.srcAttr)
        Iterator((triplet.dstId, triplet.srcAttr))
      else
        Iterator.empty
    }
    val initialComponentId = Long.MinValue
    val vertexDataSelector = (vertexId: VertexId, componentId: Long, highestComponentIdSent: Long) => Math.max(componentId, highestComponentIdSent)
    val takeHighestComponentId = (componentId1: Long, componentId2: Long) => Math.max(componentId1, componentId2)
    logInfo("Starting Pregel Operation")
    Pregel(componentGraph, initialComponentId, activeDirection = EdgeDirection.Either)(vertexDataSelector, sendComponentIdUpdate, takeHighestComponentId)
  }
}
