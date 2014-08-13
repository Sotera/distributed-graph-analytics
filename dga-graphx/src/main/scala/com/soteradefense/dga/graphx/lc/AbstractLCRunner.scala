package com.soteradefense.dga.graphx.lc

import com.soteradefense.dga.graphx.harness.Harness
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

abstract class AbstractLCRunner extends Harness with Serializable {

  override type R = S

  override def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): R = {
    save(LeafCompressionCore.lc(graph))
  }

}
