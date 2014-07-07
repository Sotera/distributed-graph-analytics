package com.soteradefense.dga.graphx.harness

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag


trait Harness{
  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long])

  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit
}
