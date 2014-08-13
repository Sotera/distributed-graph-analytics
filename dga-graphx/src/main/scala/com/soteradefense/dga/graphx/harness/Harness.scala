package com.soteradefense.dga.graphx.harness

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag


trait Harness{

  type R
  type S

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): R

  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S

}
