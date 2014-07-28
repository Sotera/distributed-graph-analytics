package com.soteradefense.dga.graphx.hbse

import junit.framework.TestCase
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class HighBetweennessCoreTest extends TestCase {

  var sc: SparkContext = null

  @Before
  override def setUp() {
    val conf = new SparkConf().setMaster("local").setAppName(this.getName).set("fs.default.name", "")
    sc = new SparkContext(conf)
  }

  @Test
  def testTwoNodesWithBetweennessGreaterThanZero() = {
    val data = Array("1,2", "1,3", "1,4", "1,5", "2,3", "2,4", "2,1", "2,3", "3,1", "3,2")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val (betweennessSet, resultGraph) = HBSECore.hbse(sc, graph)
    assert(betweennessSet.filter(_._2 > 0.0).count() == 2)
    assert(betweennessSet.filter(_._2 == 0.0).count() == 3)

  }

  @Test
  def testAllNodesWithBetweennessZero() = {
    val data = Array("1,2", "1,3", "1,4", "1,5", "1,6")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val (betweennessSet, resultGraph) = HBSECore.hbse(sc, graph)
    assert(betweennessSet.filter(_._2 == 0.0).count() == 6)
  }

  @Test
  def testOneNodeWithHighBetweenness() = {
    val data = Array("2,1", "3,1", "4,1", "5,1", "6,1", "1,7")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val (betweennessSet, resultGraph) = HBSECore.hbse(sc, graph)
    assert(betweennessSet.filter(_._2 == 0.0).count() == 6)
    assert(betweennessSet.filter(_._2 > 0.0).count() == 1)
  }

  @After
  override def tearDown() {
    sc.stop()
  }
}
