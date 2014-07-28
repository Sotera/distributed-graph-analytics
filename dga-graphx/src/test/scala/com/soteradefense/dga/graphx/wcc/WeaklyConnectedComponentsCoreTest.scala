package com.soteradefense.dga.graphx.wcc

import junit.framework.TestCase
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class WeaklyConnectedComponentsCoreTest extends TestCase {

  var sc: SparkContext = null

  @Before
  override def setUp() {
    val conf = new SparkConf().setMaster("local").setAppName(this.getName)
    sc = new SparkContext(conf)
  }

  @Test
  def testWCCSameComponent() {
    val data = Array("1,2", "2,3", "3,4", "4,5", "5,6")
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
    val data = Array(
      "1,2", "2,3", "3,4", "4,5", "5,6",
      "7,8", "8,9")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val result = WeaklyConnectionComponentsCore.wcc(graph)
    val nineComponent: VertexId = 9
    val sixComponent: VertexId = 6

    val components = result.vertices.groupBy(_._2).map(f => f)
    val nine = components.filter(f => f._1.equals(nineComponent)).first()
    assert(nine._2.count(_ => true).equals(3))

    val six = components.filter(f => f._1.equals(sixComponent)).first()
    assert(six._2.count(_ => true).equals(6))
  }

  @Test
  def testMultipleComponents() {
    val data = Array(
      "1,2", "2,3", "3,4", "4,5", "5,6",
      "7,8", "8,9",
      "10,11", "11,12", "12,10", "10,13", "13,14")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val result = WeaklyConnectionComponentsCore.wcc(graph)
    val sixComponent: VertexId = 6
    val nineComponent: VertexId = 9
    val fourteenComponent: VertexId = 14

    val components = result.vertices.groupBy(_._2).map(f => f)
    val six = components.filter(f => f._1.equals(sixComponent)).first()
    assert(six._2.count(_ => true).equals(6))

    val nine = components.filter(f => f._1.equals(nineComponent)).first()
    assert(nine._2.count(_ => true).equals(3))

    val fourteen = components.filter(f => f._1.equals(fourteenComponent)).first()
    assert(fourteen._2.count(_ => true).equals(5))
  }

  @After
  override def tearDown() {
    sc.stop()
  }

}
