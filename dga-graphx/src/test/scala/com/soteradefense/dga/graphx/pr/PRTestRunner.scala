package com.soteradefense.dga.graphx.pr

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class PRTestRunner(delta: Double) extends AbstractPRRunner(delta) {
  override type S = Graph[Double, Long]

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = graph.asInstanceOf[Graph[Double, Long]]
}
