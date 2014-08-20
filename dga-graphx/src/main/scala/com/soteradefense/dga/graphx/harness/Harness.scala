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
package com.soteradefense.dga.graphx.harness

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * A trait for defining how to run and save an analytic.
 */
trait Harness{

  /**
   * Run method return type.
   */
  type R
  /**
   * Save method return type.
   */
  type S

  /**
   * Run method for an analytic.
   *
   * @param sc The current spark context.
   * @param graph A Graph object with an optional edge weight.
   * @tparam VD ClassTag for the vertex data type.
   * @return The type of R
   */
  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): R

  /**
   * Method for saving the results of an analytic.
   *
   * @param graph A graph of any type.
   * @tparam VD ClassTag for the vertex data.
   * @tparam ED ClassTag for the edge data.
   * @return The type of S
   */
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S

}
