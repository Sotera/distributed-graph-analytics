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

package com.soteradefense.dga.graphx.louvain

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class LouvainTestRunner(minProgress: Int, progressCounter: Int) extends AbstractLouvainRunner(minProgress, progressCounter, Array[(Int,Double)]()) {

  override type R = Graph[LouvainData, Long]
  override type S = R

  /**
   * Save the graph at the given level of compression with community labels
   * level 0 = no compression
   *
   */
  override def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long]): Unit = {}

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = graph.asInstanceOf[Graph[LouvainData, Long]]

  override def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long]): R = graph

}
