package com.soteradefense.dga.graphx.hbse

import junit.framework.TestCase
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class HighBetweennessCoreTest extends TestCase {


  @Test
  def testTwoNodesWithBetweennessGreaterThanZero() = {
    val conf = new SparkConf().setMaster("local").setAppName("WCCTest").set("fs.default.name", "")
    val data = Array("1,2", "1,3", "1,4", "1,5", "2,3", "2,4", "2,1", "2,3", "3,1", "3,2")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val (betweennessSet, resultGraph) = HBSECore.hbse(sc, graph)
    val betweennessChecksOut = betweennessSet.map(f => {
      var checksOut = false
      if (f._1 < 3) {
        checksOut = f._2 > 0.0
      }
      else {
        checksOut = f._2 == 0
      }
      checksOut
    }).reduce((a, b) => a && b)

    assert(betweennessChecksOut)
  }
}
