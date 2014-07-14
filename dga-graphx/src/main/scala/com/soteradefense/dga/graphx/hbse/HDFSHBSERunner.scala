package com.soteradefense.dga.graphx.hbse

import com.esotericsoftware.kryo.Serializer
import com.twitter.chill._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


class HDFSHBSERunner(var output_dir: String, var delimiter: String) extends Serializable {

  final val highBetweennessDirectory = "highBetweennessSetData"

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): Unit = {
    val hbseOutput = HBSECore.hbse(sc, graph)
    save(hbseOutput._1, hbseOutput._2)
  }

  def save[ED: ClassTag](betweennessSet: RDD[(Long, Double)], graph: Graph[VertexData, ED]): Unit = {
    graph.vertices.map(m=> s"${m._1}$delimiter${m._2.getApproximateBetweenness}").saveAsTextFile(output_dir)
    betweennessSet.map(f=> s"${f._1}$delimiter${f._2}").saveAsTextFile(s"$output_dir$highBetweennessDirectory")
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