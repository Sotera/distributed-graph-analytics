package com.soteradefense.dga.graphx.hbse

import com.esotericsoftware.kryo.Serializer
import com.twitter.chill._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag


class HDFSHBSERunner(var output_dir: String, var delimiter: String) extends Serializable {

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): Unit = {
    save(HBSECore.hbse(sc, graph))
  }

  def save[ED: ClassTag](graph: Graph[VertexData, ED]): Unit = {
    graph.vertices.map(m=> s"${m._1}$delimiter${m._2.getApproximateBetweenness}").saveAsTextFile(output_dir)
  }

}

class HDFSHBSERunnerSerializer extends Serializer[HDFSHBSERunner] {
  override def write(kryo: Kryo, out: Output, obj: HDFSHBSERunner): Unit = {
    kryo.writeObject(out, obj.output_dir)
    kryo.writeObject(out, obj.delimiter)
  }

  override def read(kryo: Kryo, in: Input, cls: Class[HDFSHBSERunner]): HDFSHBSERunner = {
    new HDFSHBSERunner(kryo.readObject(in, classOf[String]), kryo.readObject(in, classOf[String]))
  }
}