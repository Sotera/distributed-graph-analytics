package com.soteradefense.dga.graphx.harness

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag


trait Harness {
  def run[VD: ClassTag](graph: Graph[VD, Long])

  def save[VD: ClassTag](graph: Graph[VD, Long])
}
