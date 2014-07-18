package com.soteradefense.dga.graphx.pr

import junit.framework.TestCase
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class PageRankCoreTest extends TestCase {

  @Test
  def testPageRank() {
    val conf = new SparkConf().setMaster("local").setAppName("WCCTest")
    val data = Array("1,2", "1,3", "1,4", "1,5", "2,3", "2,4", "2,1", "2,3", "3,1", "3,2")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val result = PageRankCore.pr(graph, 0.0001)
    val highest = result.vertices.max()(new Ordering[(Long, Double)]() {
      override def compare(x: (Long, Double), y: (Long, Double)): Int = {
        y._2.compareTo(x._2)
      }
    })
    val highestId: VertexId = 5
    assert(highest._1.equals(highestId))
  }
}
