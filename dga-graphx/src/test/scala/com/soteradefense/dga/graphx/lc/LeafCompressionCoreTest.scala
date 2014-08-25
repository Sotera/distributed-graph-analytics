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
    val runner = new LCTestRunner
    val result = runner.run(sc, graph)
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
    val runner = new LCTestRunner
    val result = runner.run(sc, graph)
    val resultingVertices = result.vertices.map(f => f._1).collect()
    assert(resultingVertices.size == 3)
    assert(resultingVertices.contains(1))
    assert(resultingVertices.contains(3))
    assert(resultingVertices.contains(2))
  }

  @Test
  def testLeafCompressionOnALargerGraph() {
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
      "5,2",
      "5,4",
      "4,2")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new LCTestRunner
    val result = runner.run(sc, graph)
    val resultingVertices = result.vertices.map(f => f._1).collect()
    assert(resultingVertices.size == 5)
    assert(resultingVertices.contains(1))
    assert(resultingVertices.contains(2))
    assert(resultingVertices.contains(3))
    assert(resultingVertices.contains(4))
    assert(resultingVertices.contains(5))

  }

  def testLeafCompressionWithMultipleComponents(): Unit = {
    val data = Array(
      "1,2", "2,3", "3,4", "4,5", "5,6", "25,6", "880,25", "25,880", "880,6", "6,25", "6,880",
      "15,24", "655,24", "900,655", "400,15", "900,33",
      "7,8", "8,9", "0,198", "435,44", "9,0", "44,8", "7,9", "8,7","9,7", "9,8",
      "10,11", "11,12", "12,10", "10,13", "13,14", "11,10", "10,12", "12,11")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new LCTestRunner
    val result = runner.run(sc, graph)
    val resultingVertices = result.vertices.map(f => f._1).collect()
    assert(resultingVertices.size == 9)
    assert(resultingVertices.contains(880))
    assert(resultingVertices.contains(6))
    assert(resultingVertices.contains(25))
    assert(resultingVertices.contains(9))
    assert(resultingVertices.contains(8))
    assert(resultingVertices.contains(7))
    assert(resultingVertices.contains(10))
    assert(resultingVertices.contains(11))
    assert(resultingVertices.contains(12))

  }

  @After
  override def tearDown() {
    sc.stop()
  }
}
