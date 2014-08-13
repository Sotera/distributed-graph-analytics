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


class HDFSHBSERunner(var output_dir: String, var delimiter: String) extends AbstractHBSERunner {

  final val highBetweennessDirectory = "highBetweennessSetData"

  type H = Unit
  type S = Unit

  def save[ED: ClassTag](betweennessSet: RDD[(Long, Double)], graph: Graph[VertexData, ED]): H = {
    save(graph)
    betweennessSet.map(f => s"${f._1}$delimiter${f._2}").saveAsTextFile(s"$output_dir$highBetweennessDirectory")
  }

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = {
    graph.vertices.map(m => s"${m._1}$delimiter${m._2.asInstanceOf[VertexData].getApproximateBetweenness}").saveAsTextFile(output_dir)
  }

}

class HDFSHBSERunnerSerializer extends Serializer[HDFSHBSERunner] {
  override def write(kryo: Kryo, out: Output, obj: HDFSHBSERunner): Unit = {
    kryo.writeObject(out, obj.output_dir)
    kryo.writeObject(out, obj.delimiter)
  }

  override def read(kryo: Kryo, in: Input, cls: Class[HDFSHBSERunner]): HDFSHBSERunner = {
    new HDFSHBSERunner(kryo.readObject(in, classOf[String]), kryo.readObject(in, classOf[String]))
  }
}