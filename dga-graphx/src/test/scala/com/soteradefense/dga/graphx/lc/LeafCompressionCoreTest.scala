package com.soteradefense.dga.graphx.lc

import junit.framework.TestCase
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class LeafCompressionCoreTest extends TestCase {

  var sc: SparkContext = null

  @Before
  override def setUp() {
    val conf = new SparkConf().setMaster("local").setAppName(this.getName)
    sc = new SparkContext(conf)
  }


  @Test
  def testLeafCompressionRemovesTheEntireGraph() {
    val data = Array("1,2", "2,3", "3,4", "4,5", "5,6")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val result = LeafCompressionCore.lc(graph)
    assert(result.vertices.count() == 0)
  }

  @Test
  def testLeafCompressionCompressedToThree() {
    val data = Array("1,2", "1,3", "1,4", "1,5", "2,3", "2,4", "2,1", "2,3", "3,1", "3,2")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val result = LeafCompressionCore.lc(graph)
    assert(result.vertices.count() == 3)
  }

  @After
  override def tearDown() {
    sc.stop()
  }
}
