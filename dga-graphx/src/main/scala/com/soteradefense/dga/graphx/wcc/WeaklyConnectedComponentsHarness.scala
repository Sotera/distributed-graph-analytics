package com.soteradefense.dga.graphx.wcc

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag


abstract class WeaklyConnectedComponentsHarness extends KryoSerializable {
  def run[VD: ClassTag](graph: Graph[VD, Long]) = {
    save(WeaklyConnectionComponentsCore.wcc(graph))
  }

  def save[VD: ClassTag](graph: Graph[VD, Long])

  override def write(kryo: Kryo, output: Output): Unit

  override def read(kryo: Kryo, input: Input): Unit
}