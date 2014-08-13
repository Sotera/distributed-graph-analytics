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
    val runner = new HBSETestRunner
    val (betweennessSet, resultGraph) = runner.run(sc, graph)
    assert(betweennessSet.filter(_._2 > 0.0).count() == 2)
    assert(betweennessSet.filter(_._2 == 0.0).count() == 3)
    val hbseSet = betweennessSet.collect()
    assert(hbseSet.find(_._1 == 2).get._2 == 1.0)
    assert(hbseSet.find(_._1 == 1).get._2 == 2.0)
    assert(hbseSet.find(_._1 == 5).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 4).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 3).get._2 == 0.0)
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
    val runner = new HBSETestRunner
    val (betweennessSet, resultGraph) = runner.run(sc, graph)
    val hbseSet = betweennessSet.collect()
    assert(hbseSet.find(_._1 == 1).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 6).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 5).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 4).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 3).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 2).get._2 == 0.0)
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
    val runner = new HBSETestRunner
    val (betweennessSet, resultGraph) = runner.run(sc, graph)
    val hbseSet = betweennessSet.collect()
    assert(hbseSet.find(_._1 == 1).get._2 == 5.0)
    assert(hbseSet.find(_._1 == 7).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 6).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 5).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 4).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 3).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 2).get._2 == 0.0)
  }

  @Test
  def testHBSEWithMultipleBetweenness() = {
    val data = Array("1,2",
      "1,3",
      "1,4",
      "1,5",
      "1,6",
      "2,3",
      "2,4",
      "2,5",
      "3,4",
      "3,5",
      "4,5",
      "6,1",
      "6,7",
      "6,8",
      "6,9",
      "7,8",
      "7,9",
      "8,9",
      "9,8",
      "9,7",
      "5,2")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new HBSETestRunner
    val (betweennessSet, resultGraph) = runner.run(sc, graph)
    val hbseSet = betweennessSet.collect()
    assert(hbseSet.find(_._1 == 1).get._2 == 4.0)
    assert(hbseSet.find(_._1 == 2).get._2 == 3.0)
    assert(hbseSet.find(_._1 == 5).get._2 == 3.0)
    assert(hbseSet.find(_._1 == 6).get._2 == 3.0)
    assert(hbseSet.find(_._1 == 9).get._2 == 1.0)

    assert(hbseSet.find(_._1 == 7).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 8).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 3).get._2 == 0.0)
    assert(hbseSet.find(_._1 == 4).get._2 == 0.0)
  }

  @After
  override def tearDown() {
    sc.stop()
  }
}
