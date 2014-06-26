package com.soteradefense.dga.graphx.wcc

import com.esotericsoftware.kryo.Serializer
import com.soteradefense.dga.graphx.harness.Harness
import com.twitter.chill._
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

case class HDFSWCCRunner(var output_dir: String, var delimiter: String) extends Harness with Serializable{
  def run[VD: ClassTag](graph: Graph[VD, Long]) = {
    save(WeaklyConnectionComponentsCore.wcc(graph))
  }

  def save[VD: ClassTag](graph: Graph[VD, Long]): Unit = {
    graph.triplets.map(t => s"${t.srcId}$delimiter${t.dstId}$delimiter${t.srcAttr}").saveAsTextFile(output_dir)
  }
}

class HDFSWCCRunnerSerializer extends Serializer[HDFSWCCRunner] {
  override def write(kryo: Kryo, out: Output, obj: HDFSWCCRunner): Unit = {
    kryo.writeObject(out, obj.output_dir)
    kryo.writeObject(out, obj.delimiter)
  }

  override def read(kryo: Kryo, in: Input, cls: Class[HDFSWCCRunner]): HDFSWCCRunner = {
    new HDFSWCCRunner(kryo.readObject(in, classOf[String]), kryo.readObject(in, classOf[String]))
  }
}