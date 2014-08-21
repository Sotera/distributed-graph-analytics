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

import com.twitter.chill._
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Class for running Leaf Compression with data on hdfs.
 * @param output_dir Directory to output the results.
 * @param delimiter Character to split the result.
 */
class HDFSLCRunner(var output_dir: String, var delimiter: String) extends AbstractLCRunner {

  /**
   * The save return type is of type Unit.
   */
  override type S = Unit

  /**
   * Maps and saves each edge in the graph.
   * @param graph A graph of any type.
   * @tparam VD ClassTag for the vertex data.
   * @tparam ED ClassTag for the edge data.
   */
  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = {
    graph.triplets.map(t => s"${t.srcId}$delimiter${t.dstId}").saveAsTextFile(output_dir)
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    kryo.writeObject(out, this.output_dir)
    kryo.writeObject(out, this.delimiter)
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    this.output_dir = kryo.readObject(in, classOf[String])
    this.delimiter = kryo.readObject(in, classOf[String])
  }
}