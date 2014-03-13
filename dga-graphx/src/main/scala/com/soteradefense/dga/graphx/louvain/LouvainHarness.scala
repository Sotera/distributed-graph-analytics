package com.soteradefense.dga.graphx.louvain

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.Logging


trait  LouvainHarness {

  
  def run[VD: ClassTag](sc:SparkContext,graph:Graph[VD,Long]) = {
    
    var louvainGraph = LouvainAlgorithm.createLouvainGraph(graph)
    var level = -1
	var q = -1.0
	var halt = false
    do {
	  level += 1
	  println(s"\nStarting Louvain level $level")
	  val (currentQ,currentGraph) = LouvainAlgorithm.louvain(sc, louvainGraph)
	  louvainGraph.unpersistVertices(blocking=false)
	  louvainGraph=currentGraph
	  saveLevel(sc,level,currentQ,louvainGraph)
	  
	  if (currentQ > q + 0.0005 ){ // stop when we've stopped making reasonable progress
	    q = currentQ
	    louvainGraph = LouvainAlgorithm.compressGraph(louvainGraph)
	  }
	  else {
	    halt = true
	  }
	 
	}while ( !halt )
	finalSave(sc,level,q,louvainGraph)  
  }

  /**
   * Save the graph at the given level of compression with community labels
   * level 0 = no compression
   * 
   * override to specify save behavior
   */
  def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
    
  }
  
  /**
   * Complete any final save actions required
   * 
   * override to specify save behavior
   */
  def finalSave(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
    
  }
  
  
  
}