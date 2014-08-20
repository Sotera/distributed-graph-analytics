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

import com.esotericsoftware.kryo.Serializer
import com.twitter.chill._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Object for running hbse and saving to hdfs.
 *
 * @param output_dir Directory to output the results.
 * @param delimiter Delimiter that splits the data.
 */
class HDFSHBSERunner(var output_dir: String, var delimiter: String) extends AbstractHBSERunner {

  /**
   * Directory to write the betweenness set to.
   */
  final val highBetweennessDirectory = "highBetweennessSetData"

  /**
   * Return type of the hbse save method.
   */
  type H = Unit
  /**
   * Return type of the harness save method.
   */
  type S = Unit

  /**
   * Saves a graph to HDFS and saves the HBSE Set to HDFS.
   *
   * @param betweennessSet Set of the highest betweenness values.
   * @param graph A graph that ran through HBSE.
   * @tparam ED An edge data type.
   */
  def save[ED: ClassTag](betweennessSet: RDD[(Long, Double)], graph: Graph[HBSEData, ED]): H = {
    save(graph)
    betweennessSet.map(f => s"${f._1}$delimiter${f._2}").saveAsTextFile(s"$output_dir$highBetweennessDirectory")
  }

  /**
   * Saves the graph to HDFS in the format of VertexId[delimiter]ApproxBetweenness.
   *
   * @param graph A graph of any type.
   * @tparam VD ClassTag for the vertex data.
   * @tparam ED ClassTag for the edge data.
   */
  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = {
    graph.vertices.map(m => s"${m._1}$delimiter${m._2.asInstanceOf[HBSEData].getApproximateBetweenness}").saveAsTextFile(output_dir)
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