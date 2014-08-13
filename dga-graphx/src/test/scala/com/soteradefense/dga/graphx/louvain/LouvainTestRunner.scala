package com.soteradefense.dga.graphx.louvain

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class LouvainTestRunner(minProgress: Int, progressCounter: Int) extends AbstractLouvainRunner(minProgress, progressCounter) {

  override type R = Graph[VertexState, Long]
  override type S = R

  /**
   * Save the graph at the given level of compression with community labels
   * level 0 = no compression
   *
   */
  override def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]): Unit = {}

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = graph.asInstanceOf[Graph[VertexState, Long]]

  override def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]): R = graph

}
