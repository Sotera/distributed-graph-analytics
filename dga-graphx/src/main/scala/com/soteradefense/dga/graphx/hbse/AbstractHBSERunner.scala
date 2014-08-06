package com.soteradefense.dga.graphx.hbse

import com.soteradefense.dga.graphx.harness.Harness
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class AbstractHBSERunner extends Harness with Serializable {

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): Unit = {
    val hbseOutput = HBSECore.hbse(sc, graph)
    save(hbseOutput._1, hbseOutput._2)
  }

  def save[ED: ClassTag](betweennessSet: RDD[(Long, Double)], graph: Graph[VertexData, ED])
}
