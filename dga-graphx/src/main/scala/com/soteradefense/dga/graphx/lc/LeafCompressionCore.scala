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
package com.soteradefense.dga.graphx.lc

import org.apache.spark.Logging
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

/**
 * Core class for running the leaf compression algorithm.
 */
class LeafCompressionCore extends Logging with Serializable {
  /**
   * Runs the leaf compression algorithm.
   * @param graph A graph
   * @tparam VD type of the vertex attribute
   * @tparam ED type of the edge attribute
   * @return A compressed graph.
   */
  def runLeafCompression[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Long, Long] = {
    logInfo("Creating a graph for Leaf Compression")
    val previousGraph = graph.outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0).toLong}.mapTriplets(e => e.srcAttr).cache()
    logInfo("Trimming Nodes off of the previous graph")
    val compressedGraph = previousGraph.subgraph((e: EdgeTriplet[Long, Long]) => e.attr != 1 && e.attr != 0, (vid: VertexId, vdata: Long) => vdata != 0 && vdata != 1)
    val graphChanged = previousGraph.vertices.count() != compressedGraph.vertices.count()
    logInfo(s"Graph Changed: $graphChanged")
    if (graphChanged)
      runLeafCompression(compressedGraph)
    else
      compressedGraph
  }
}
