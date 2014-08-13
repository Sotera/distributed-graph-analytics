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
    val one: VertexId = 1
    val two: VertexId = 2
    val three: VertexId = 3

    val oneNode = result.vertices.filter(pred => pred._1.equals(one)).first()
    val twoNode = result.vertices.filter(pred => pred._1.equals(two)).first()
    val threeNode = result.vertices.filter(pred => pred._1.equals(three)).first()
    assert(oneNode._2 == twoNode._2 && oneNode._2 == threeNode._2)
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
    val one: VertexId = 1
    val highest = result.vertices.max()(new Ordering[(Long, Double)]() {
      override def compare(x: (Long, Double), y: (Long, Double)): Int = {
        x._2.compareTo(y._2)
      }
    })
    assert(highest._1 == one)
  }

  @After
  override def tearDown() {
    sc.stop()
  }
}
