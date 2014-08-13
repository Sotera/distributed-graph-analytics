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
package com.soteradefense.dga.graphx.louvain

import com.soteradefense.dga.graphx.harness.Harness
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

abstract class AbstractLouvainRunner(var minProgress: Int, var progressCounter: Int) extends Harness with Serializable {

  var qValues = Array[(Int, Double)]()

  override def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): R = {

    var louvainGraph = LouvainCore.createLouvainGraph(graph)

    var level = -1 // number of times the graph has been compressed
    var q = -1.0 // current modularity value
    var halt = false
    do {
      level += 1
      println(s"\nStarting Louvain level $level")

      // label each vertex with its best community choice at this level of compression
      val (currentQ, currentGraph, passes) = LouvainCore.louvain(sc, louvainGraph, minProgress, progressCounter)
      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph

      saveLevel(sc, level, currentQ, louvainGraph)

      // If modularity was increased by at least 0.001 compress the graph and repeat
      // halt immediately if the community labeling took less than 3 passes
      //println(s"if ($passes > 2 && $currentQ > $q + 0.001 )")
      if (passes > 2 && currentQ > q + 0.001) {
        q = currentQ
        louvainGraph = LouvainCore.compressGraph(louvainGraph)
      }
      else {
        halt = true
      }

    } while (!halt)
    finalSave(sc, level, q, louvainGraph)
  }

  /**
   * Save the graph at the given level of compression with community labels
   * level 0 = no compression
   *
   */
  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long])

  def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]): R

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S
}