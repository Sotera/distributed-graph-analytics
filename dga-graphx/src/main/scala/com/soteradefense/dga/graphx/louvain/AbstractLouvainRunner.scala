package com.soteradefense.dga.graphx.louvain

import com.soteradefense.dga.graphx.harness.Harness
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

abstract class AbstractLouvainRunner(var minProgress: Int, var progressCounter: Int) extends Harness with Serializable {

  var qValues = Array[(Int, Double)]()

  override def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]) = {

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

  def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long])

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED])
}