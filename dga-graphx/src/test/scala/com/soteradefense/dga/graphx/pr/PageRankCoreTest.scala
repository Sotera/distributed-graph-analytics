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
    val runner = new PRTestRunner(0.001)
    val result = runner.run(sc, graph)
    val one = getVertex(1, result)
    val two = getVertex(2, result)
    val three = getVertex(3, result)
    val four = getVertex(4, result)
    val five = getVertex(5, result)
    assert(one._2 == two._2 && one._2 == three._2)
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
    val runner = new PRTestRunner(0.001)
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
    val runner = new PRTestRunner(0.001)
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
    val runner = new PRTestRunner(0.01)
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

  @Test
  def testGraphWithMultipleComponents(): Unit = {
    val data = Array(
      "1,2", "2,3", "3,4", "4,5", "5,6", "25,6", "880,25",
      "15,24", "655,24", "900,655", "400,15", "900,33",
      "7,8", "8,9", "0,198", "435,44", "9,0", "44,8",
      "10,11", "11,12", "12,10", "10,13", "13,14")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new PRTestRunner(0.001)
    val result = runner.run(sc, graph)
    val v15 = getVertex(15, result)
    val v4 = getVertex(4, result)
    val v1 = getVertex(1, result)
    val v880 = getVertex(880, result)
    val v900 = getVertex(900, result)
    val v3 = getVertex(3, result)
    val v7 = getVertex(7, result)
    val v44 = getVertex(44, result)
    val v400 = getVertex(400, result)
    val v435 = getVertex(435, result)
    val v5 = getVertex(5, result)
    val v2 = getVertex(2, result)
    val v8 = getVertex(8, result)
    val v6 = getVertex(6, result)
    val v9 = getVertex(9, result)
    val v0 = getVertex(0, result)
    val v198 = getVertex(198, result)
    val v10 = getVertex(10, result)
    val v14 = getVertex(14, result)
    val v24 = getVertex(24, result)
    val v12 = getVertex(12, result)
    val v13 = getVertex(13, result)
    val v11 = getVertex(11, result)
    val v33 = getVertex(33, result)
    val v655 = getVertex(655, result)
    assert(v8._2 == v6._2)
    assert(v9._2 > v0._2)
    assert(v0._2 > v198._2)
    assert(v198._2 > v10._2)
    assert(v14._2 < v24._2)
    assert(v14._2 == v12._2)
    assert(v13._2 == v11._2)
    assert(v13._2 < v33._2)
    assert(v33._2 == v655._2)
    assert(v15._2 == v4._2)
    assert(v15._2 == v1._2)
    assert(v15._2 == v880._2)
    assert(v15._2 == v900._2)
    assert(v15._2 == v3._2)
    assert(v15._2 == v7._2)
    assert(v15._2 == v44._2)
    assert(v15._2 == v400._2)
    assert(v15._2 == v435._2)
    assert(v15._2 == v5._2)
    assert(v15._2 == v2._2)

  }

  def getVertex(id: Long, result: Graph[Double, Long]) = {
    result.vertices.filter(pred => pred._1 == id.asInstanceOf[VertexId]).first()
  }

  @After
  override def tearDown() {
    sc.stop()
  }
}
