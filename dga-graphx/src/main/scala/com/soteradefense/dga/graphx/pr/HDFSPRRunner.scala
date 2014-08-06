package com.soteradefense.dga.graphx.pr

import com.esotericsoftware.kryo.Serializer
import com.twitter.chill._
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag


class HDFSPRRunner(var output_dir: String, var delimiter: String, delta: Double) extends AbstractPRRunner(delta) {

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) = {
    graph.vertices.map(f => s"${f._1}${delimiter}${f._2}").saveAsTextFile(output_dir)
  }
}

class HDFSPRRunnerSerializer extends Serializer[HDFSPRRunner] {
  override def write(kryo: Kryo, out: Output, obj: HDFSPRRunner): Unit = {
    kryo.writeObject(out, obj.output_dir)
    kryo.writeObject(out, obj.delimiter)
    out.writeDouble(obj.delta)
  }

  override def read(kryo: Kryo, in: Input, cls: Class[HDFSPRRunner]): HDFSPRRunner = {
    new HDFSPRRunner(kryo.readObject(in, classOf[String]), kryo.readObject(in, classOf[String]), in.readDouble())
  }
}