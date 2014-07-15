package com.soteradefense.dga.graphx.louvain

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.soteradefense.dga.graphx.harness.Harness
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
class HDFSLouvainRunner(var minProgress: Int, var progressCounter: Int, var outputdir: String) extends Harness with Serializable {

  var qValues = Array[(Int, Double)]()
  var vertexSavePath: String = outputdir
  var edgeSavePath: String = outputdir

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]) = {

    var louvainGraph = LouvainCore.createLouvainGraph(graph)

    var level = -1 // number of times the graph has been compressed
    var q = -1.0 // current modularity value
    var halt = false
    do {
      level += 1
      println(s"\nStarting Louvain level $level")

      // label each vertex with its best community choice at this level of compression
      val (currentQ, currentGraph, passes) = LouvainCore.louvain(sc, louvainGraph, minProgress, progressCounter)
      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph

      saveLevel(sc, level, currentQ, louvainGraph)

      // If modularity was increased by at least 0.001 compress the graph and repeat
      // halt immediately if the community labeling took less than 3 passes
      //println(s"if ($passes > 2 && $currentQ > $q + 0.001 )")
      if (passes > 2 && currentQ > q + 0.001) {
        q = currentQ
        louvainGraph = LouvainCore.compressGraph(louvainGraph)
      }
      else {
        halt = true
      }

    } while (!halt)
    finalSave(sc, level, q, louvainGraph)
  }

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

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
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