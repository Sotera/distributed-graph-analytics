package com.soteradefense.dga.graphx.pr

import com.soteradefense.dga.graphx.harness.Harness
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

abstract class AbstractPRRunner(var delta: Double) extends Harness with Serializable {
  override def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): Unit = {
    save(PageRankCore.pr(graph, delta))
  }

  def runGraphXImplementation[VD: ClassTag](graph: Graph[VD, Long]): Unit = {
    save(PageRankCore.prGraphX(graph, delta))
  }
}
