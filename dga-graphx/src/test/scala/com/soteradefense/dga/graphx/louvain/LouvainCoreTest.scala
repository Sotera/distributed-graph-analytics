package com.soteradefense.dga.graphx.louvain

import junit.framework.TestCase
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class LouvainCoreTest extends TestCase {

  var sc: SparkContext = null

  @Before
  override def setUp() {
    val conf = new SparkConf().setMaster("local").setAppName(this.getName)
    sc = new SparkContext(conf)
  }

  @Test
  def testLouvainOneCommunity() {
    val data = Array("1,2,1", "2,3,1", "3,4,1", "4,5,1", "5,6,1")
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

  @Test
  def testLouvainMultipleCommunities() {
    val data = Array("1,2,1", "2,3,1", "3,4,1", "4,5,1", "5,6,1", "10,16,1", "10,11,1", "10,12,1", "10,13,1", "10,14,1", "10,15,1")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong, tokens(2).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val louvainGraph = LouvainCore.createLouvainGraph(graph)
    val result = LouvainCore.louvain(sc, louvainGraph)
    assert(result._2.vertices.filter(f => f._2.community == 1).count() == 2)
    assert(result._2.vertices.filter(f => f._2.community == 4).count() == 2)
    assert(result._2.vertices.filter(f => f._2.community == 5).count() == 2)
    assert(result._2.vertices.filter(f => f._2.community == 10).count() == 7)
  }

  @After
  override def tearDown() {
    sc.stop()
  }

}