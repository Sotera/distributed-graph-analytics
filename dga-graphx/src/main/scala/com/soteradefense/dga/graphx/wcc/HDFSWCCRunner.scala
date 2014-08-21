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

import com.twitter.chill._
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Class for running weakly connected components with hdfs.
 * @param output_dir The directory to save the results.
 * @param delimiter The delimiter to split the results.
 */
class HDFSWCCRunner(var output_dir: String, var delimiter: String) extends AbstractWCCRunner {

  /**
   * The return type for S is Unit.
   */
  override type S = Unit

  /**
   * Saves the graph and it's component ID to HDFS.
   * @param graph A graph of any type.
   * @tparam VD ClassTag for the vertex data.
   * @tparam ED ClassTag for the edge data.
   */
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = {
    graph.triplets.map(t => s"${t.srcId}$delimiter${t.dstId}$delimiter${t.srcAttr}").saveAsTextFile(output_dir)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.output_dir)
    kryo.writeObject(output, this.delimiter)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    this.output_dir = kryo.readObject(input, classOf[String])
    this.delimiter = kryo.readObject(input, classOf[String])
  }
}