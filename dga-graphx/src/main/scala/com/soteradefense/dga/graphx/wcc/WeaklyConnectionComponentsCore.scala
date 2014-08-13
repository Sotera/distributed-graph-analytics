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
