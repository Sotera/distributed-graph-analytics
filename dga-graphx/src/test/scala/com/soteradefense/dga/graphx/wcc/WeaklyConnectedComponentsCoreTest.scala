package com.soteradefense.dga.graphx.wcc

import junit.framework.TestCase
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.Test

class WeaklyConnectedComponentsCoreTest extends TestCase {

  @Test
  def testWCCSameComponent() {
    val conf = new SparkConf().setMaster("local").setAppName("WCCTest")
    val data = Array("1,2", "2,3", "3,4", "4,5", "5,6")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val result = WeaklyConnectionComponentsCore.wcc(graph)
    val vertexId: VertexId = 6
    assert(result.vertices.map(f => f._2).reduce((a, b) => if (a.equals(b)) a else -1).equals(vertexId))
  }

  @Test
  def testWCCDifferentComponent() {
    val conf = new SparkConf().setMaster("local").setAppName("WCCTest")
    val data = Array("1,2", "2,3", "3,4", "4,5", "5,6", "7,8", "8,9")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val result = WeaklyConnectionComponentsCore.wcc(graph)
    val vertexId: VertexId = 6
    assert(!result.vertices.map(f => f._2).reduce((a, b) => if (a.equals(b)) a else -1).equals(vertexId))
  }

}
