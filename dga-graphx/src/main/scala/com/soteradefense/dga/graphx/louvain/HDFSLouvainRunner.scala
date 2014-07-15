package com.soteradefense.dga.graphx.louvain

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
class HDFSLouvainRunner(minProgress: Int, progressCounter: Int, outputdir: String) extends LouvainHarness(minProgress: Int, progressCounter: Int) {

  var qValues = Array[(Int, Double)]()
  var vertexSavePath: String = outputdir
  var edgeSavePath: String = outputdir

  override def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {
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

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    graph.vertices.saveAsTextFile(vertexSavePath)
    graph.edges.saveAsTextFile(edgeSavePath)
  }
}