package com.soteradefense.dga.graphx.lc

import com.esotericsoftware.kryo.Serializer
import com.soteradefense.dga.graphx.harness.Harness
import com.twitter.chill._
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag


class HDFSLCRunner(var output_dir: String, var delimiter: String) extends Harness with Serializable {
  override def run[VD: ClassTag](graph: Graph[VD, Long]): Unit = {
    save(LeafCompressionCore.lc(graph))
  }

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    graph.triplets.map(t => s"${t.srcId}$delimiter${t.dstId}").saveAsTextFile(output_dir)
  }
}

class HDFSLCRunnerSerializer extends Serializer[HDFSLCRunner] {
  override def write(kryo: Kryo, out: Output, obj: HDFSLCRunner): Unit = {
    kryo.writeObject(out, obj.output_dir)
    kryo.writeObject(out, obj.delimiter)
  }

  override def read(kryo: Kryo, in: Input, cls: Class[HDFSLCRunner]): HDFSLCRunner = {
    new HDFSLCRunner(kryo.readObject(in, classOf[String]), kryo.readObject(in, classOf[String]))
  }
}