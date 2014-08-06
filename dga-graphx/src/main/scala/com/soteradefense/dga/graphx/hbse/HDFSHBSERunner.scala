package com.soteradefense.dga.graphx.hbse

import com.esotericsoftware.kryo.Serializer
import com.twitter.chill._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


class HDFSHBSERunner(var output_dir: String, var delimiter: String) extends AbstractHBSERunner {

  final val highBetweennessDirectory = "highBetweennessSetData"

  def save[ED: ClassTag](betweennessSet: RDD[(Long, Double)], graph: Graph[VertexData, ED]): Unit = {
    save(graph)
    betweennessSet.map(f => s"${f._1}$delimiter${f._2}").saveAsTextFile(s"$output_dir$highBetweennessDirectory")
  }

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    graph.vertices.map(m => s"${m._1}$delimiter${m._2.asInstanceOf[VertexData].getApproximateBetweenness}").saveAsTextFile(output_dir)
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