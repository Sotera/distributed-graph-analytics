package com.soteradefense.dga.graphx.wcc

import com.soteradefense.dga.graphx.harness.Harness
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

abstract class AbstractWCCRunner extends Harness with Serializable {

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]) = {
    save(WeaklyConnectionComponentsCore.wcc(graph))
  }

  def runGraphXImplementation[VD: ClassTag](graph: Graph[VD, Long]) = {
    save(WeaklyConnectionComponentsCore.wccGraphX(graph))
  }
}
