package com.soteradefense.dga.graphx.louvain

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.Array.canBuildFrom


class HDFSLouvainRunner(outputdir:String) extends LouvainHarness{

  var qValues = Array[(Int,Double)]()
      
  override def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
	  graph.vertices.saveAsTextFile(outputdir+"/level_"+level+"_vertices")
      graph.edges.saveAsTextFile(outputdir+"/level_"+level+"_edges")
      qValues = qValues :+ ((level,q))
      println(s"qValue: $q")
        
      // overwrite the q values at each level
      sc.parallelize(qValues, 1).saveAsTextFile(outputdir+"/qvalues")
  }
  
}