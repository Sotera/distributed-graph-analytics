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
    val runner = new WCCTestRunner
    val result = runner.run(sc, graph)
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
    val runner = new WCCTestRunner
    val result = runner.run(sc, graph)
    val nineComponent: VertexId = 9
    val sixComponent: VertexId = 6

    val components = result.vertices.groupBy(_._2).map(f => f)
    val nine = components.filter(f => f._1.equals(nineComponent)).first()
    assert(nine._2.size.equals(3))

    val six = components.filter(f => f._1.equals(sixComponent)).first()
    assert(six._2.size.equals(6))
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
    val runner = new WCCTestRunner
    val result = runner.run(sc, graph)
    val sixComponent: VertexId = 6
    val nineComponent: VertexId = 9
    val fourteenComponent: VertexId = 14

    val components = result.vertices.groupBy(_._2).map(f => f)
    val six = components.filter(f => f._1.equals(sixComponent)).first()
    assert(six._2.size.equals(6))

    val nine = components.filter(f => f._1.equals(nineComponent)).first()
    assert(nine._2.size.equals(3))

    val fourteen = components.filter(f => f._1.equals(fourteenComponent)).first()
    assert(fourteen._2.size.equals(5))
  }

  @Test
  def testBiggerGraph() {
    val data = Array(
      "1,2", "2,3", "3,4", "4,5", "5,6", "25,6", "880,25",
      "15,24","655,24","900,655","400,15", "900,33",
      "7,8", "8,9","0,198","435,44","9,0", "44,8",
      "10,11", "11,12", "12,10", "10,13", "13,14")
    val rdd = sc.parallelize(data.toSeq)
    val edgeRDD: RDD[Edge[Long]] = rdd.map(f => {
      val tokens = f.split(",")
      new Edge(tokens(0).toLong, tokens(1).toLong)
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new WCCTestRunner
    val result = runner.run(sc, graph)
    val component1: VertexId = 880
    val component2: VertexId = 900
    val component3: VertexId = 14
    val component4: VertexId = 435

    val components = result.vertices.groupBy(_._2).map(f => f)
    val component1Count = components.filter(f => f._1.equals(component1)).first()
    assert(component1Count._2.size.equals(8))

    val component2Count = components.filter(f => f._1.equals(component2)).first()
    assert(component2Count._2.size.equals(6))

    val component3Count = components.filter(f => f._1.equals(component3)).first()
    assert(component3Count._2.size.equals(5))

    val component4Count = components.filter(f => f._1.equals(component4)).first()
    assert(component4Count._2.size.equals(7))

  }

  @After
  override def tearDown() {
    sc.stop()
  }

}
