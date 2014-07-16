package com.soteradefense.dga.graphx.wcc

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.mutable._

class WeaklyConnectedComponentsCoreTest extends SpecificationWithJUnit {

  sequential
  "Tests" should {
    "Test that all vertices are in the same component." ! {
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
      var sameComponent = true
      val vertexId: VertexId = 6
      result.vertices.map(f => f._2).reduce((a, b) => if (a.equals(b)) a else -1).equals(vertexId)
    }
    "Test that all vertices are not in the same component." ! {
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
      var sameComponent = true
      val vertexId: VertexId = 6
      !result.vertices.map(f => f._2).reduce((a, b) => if (a.equals(b)) a else -1).equals(vertexId)
    }
  }
}
