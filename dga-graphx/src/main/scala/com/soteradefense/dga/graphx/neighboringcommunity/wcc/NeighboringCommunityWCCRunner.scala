package com.soteradefense.dga.graphx.neighboringcommunity.wcc

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.soteradefense.dga.graphx.wcc.AbstractWCCRunner
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class NeighboringCommunityWCCRunner extends AbstractWCCRunner {

  override type S = Graph[Long, Long]

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = graph.asInstanceOf[Graph[Long, Long]]

  override def write(p1: Kryo, p2: Output): Unit = {
    // Nothing needs written.
  }

  override def read(p1: Kryo, p2: Input): Unit = {
    // Nothing needs read.
  }
}
