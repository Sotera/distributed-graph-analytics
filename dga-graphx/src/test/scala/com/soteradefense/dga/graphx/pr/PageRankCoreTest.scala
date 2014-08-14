/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.soteradefense.dga.graphx.pr

import junit.framework.TestCase
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class PageRankCoreTest extends TestCase {

  var sc: SparkContext = null

  @Before
  override def setUp() {
    val conf = new SparkConf().setMaster("local").setAppName(this.getName)
    sc = new SparkContext(conf)
  }

  @Test
  def testPageRankWithThreeNodesEqual() {
    val data = Array("1,2", "1,3", "1,4", "1,5", "2,3", "2,4", "2,1", "2,3", "3,1", "3,2")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new PRTestRunner(0.0001)
    val result = runner.run(sc, graph)
    val one = getVertex(1, result)
    val two = getVertex(2, result)
    val three = getVertex(3, result)
    val four = getVertex(4, result)
    val five = getVertex(5, result)
    assert(one._2 == two._2 && one._2 == three._2 && three._2 == four._2 && four._2 == five._2)
  }

  @Test
  def testPageRankWithOneNodeMaster() {
    val data = Array("2,1", "3,1", "4,1", "5,1", "6,1")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new PRTestRunner(0.0001)
    val result = runner.run(sc, graph)
    val one = getVertex(1, result)
    val two = getVertex(2, result)
    val three = getVertex(3, result)
    val four = getVertex(4, result)
    val five = getVertex(5, result)
    assert(two._2 == three._2 && three._2 == four._2 && four._2 == five._2)
    assert(one._2 > two._2)
  }

  @Test
  def testCyclicGraph() {
    val data = Array("1,2", "2,1", "2,3", "3,2", "3,1", "1,3")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new PRTestRunner(0.0001)
    val result = runner.run(sc, graph)
    val one = getVertex(1, result)
    val two = getVertex(2, result)
    val three = getVertex(3, result)

    assert(one._2 == two._2 && one._2 == three._2)
  }

  @Test
  def testLargerGraph() {
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
    val runner = new PRTestRunner(0.001)
    val result = runner.run(sc, graph)
    val one = getVertex(1, result)
    val two = getVertex(2, result)
    val three = getVertex(3, result)
    val four = getVertex(4, result)
    val five = getVertex(5, result)
    val six = getVertex(6, result)
    val seven = getVertex(7, result)
    val eight = getVertex(8, result)
    val nine = getVertex(9, result)
    assert(one._2 == five._2)
    assert(five._2 == seven._2)
    assert(two._2 == nine._2)
    assert(seven._2 == three._2)
    assert(three._2 == four._2)
    assert(six._2 == three._2)
    assert(six._2 == eight._2)
  }

  def getVertex(id: Long, result: Graph[Double, Long]) = {
    result.vertices.filter(pred => pred._1 == id.asInstanceOf[VertexId]).first()
  }

  @After
  override def tearDown() {
    sc.stop()
  }
}
