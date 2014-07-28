package com.soteradefense.dga.graphx.louvain

import junit.framework.TestCase
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class LouvainCoreTest extends TestCase {

  @Test
  def testLouvain() {
    val conf = new SparkConf().setMaster("local").setAppName("WCCTest")
    val data = Array("1,2,1", "2,3,1", "3,4,1", "4,5,1", "5,6,1")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong, tokens(2).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val louvainGraph = LouvainCore.createLouvainGraph(graph)
    val result = LouvainCore.louvain(sc, louvainGraph)
    assert(result._2.vertices.map(m => m._2.community == 1).reduce((a, b) => a == b))
  }

}
