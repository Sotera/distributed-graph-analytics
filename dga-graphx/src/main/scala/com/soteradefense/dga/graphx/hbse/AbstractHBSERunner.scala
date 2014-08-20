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
package com.soteradefense.dga.graphx.hbse

import com.esotericsoftware.kryo.KryoSerializable
import com.soteradefense.dga.graphx.harness.Harness
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * An abstract class that runs high betweenness set extraction.
 */
abstract class AbstractHBSERunner extends Harness with Serializable with KryoSerializable {

  /**
   * Return type for the high betweenness save method.
   */
  type H
  /**
   * Overrides the run return type to be the same type as H.
   */
  override type R = H

  /**
   * Run method for High Betweenness Set Extraction.  It automatically calls the save methods.
   *
   * @param sc The current spark context.
   * @param graph A graph to run hbse on.
   * @tparam VD The vertex data type.
   * @return The type specified by H.
   */
  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): R = {
    val hbseCore = new HighBetweennessCore(sc.getConf)
    val hbseOutput = hbseCore.runHighBetweennessSetExtraction(sc, graph)
    save(hbseOutput._1, hbseOutput._2)
  }

  /**
   * Abstract method for saving a high betweenness set extraction graph and set.
   * @param betweennessSet Set of the highest betweenness values.
   * @param graph A graph that ran through HBSE.
   * @tparam ED An edge data type.
   * @return The type specified by H.
   */
  def save[ED: ClassTag](betweennessSet: RDD[(Long, Double)], graph: Graph[HBSEData, ED]): H
}
