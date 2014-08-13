package com.soteradefense.dga.graphx.louvain

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.Array.canBuildFrom
import scala.reflect.ClassTag

/**
 * Execute the louvain algorithim and save the vertices and edges in hdfs at each level.
 * Can also save locally if in local mode.
 *
 * See LouvainHarness for algorithm details
 */
class HDFSLouvainRunner(minProgress: Int, progressCounter: Int, var outputdir: String) extends AbstractLouvainRunner(minProgress, progressCounter) {

  var vertexSavePath: String = outputdir
  var edgeSavePath: String = outputdir
  override type R = Unit
  override type S = Unit
  /**
   * Save the graph at the given level of compression with community labels
   * level 0 = no compression
   *
   */
  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {
    vertexSavePath = outputdir + "/level_" + level + "_vertices"
    edgeSavePath = outputdir + "/level_" + level + "_edges"
    save(graph)
    //graph.vertices.map( {case (id,v) => ""+id+","+v.internalWeight+","+v.community }).saveAsTextFile(outputdir+"/level_"+level+"_vertices")
    //graph.edges.mapValues({case e=>""+e.srcId+","+e.dstId+","+e.attr}).saveAsTextFile(outputdir+"/level_"+level+"_edges")
    qValues = qValues :+ ((level, q))
    println(s"qValue: $q")

    // overwrite the q values at each level
    sc.parallelize(qValues, 1).saveAsTextFile(outputdir + "/qvalues")
  }

  /**
   * Complete any final save actions required
   *
   */
  def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {

  }

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = {
    graph.vertices.saveAsTextFile(vertexSavePath)
    graph.edges.saveAsTextFile(edgeSavePath)
  }
}

class HDFSLouvainRunnerSerializer extends Serializer[HDFSLouvainRunner] {
  override def write(kryo: Kryo, output: Output, obj: HDFSLouvainRunner): Unit = {
    kryo.writeObject(output, obj.minProgress)
    kryo.writeObject(output, obj.progressCounter)
    kryo.writeObject(output, obj.outputdir)
    //TODO: Maybe need to serialize qValues
  }

  override def read(kryo: Kryo, input: Input, classType: Class[HDFSLouvainRunner]): HDFSLouvainRunner = {
    new HDFSLouvainRunner(kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[String]))
  }
}