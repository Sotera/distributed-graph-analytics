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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.Array.canBuildFrom
import scala.reflect.ClassTag

/**
 * Execute the louvain algorithim and save the vertices and edges in hdfs at each level.
 *
 * @param minProgress Minimum compression progress.
 * @param progressCounter Progress counter.
 * @param outputdir Directory to output the results.
 */
class HDFSLouvainRunner(minProgress: Int, progressCounter: Int, var outputdir: String) extends AbstractLouvainRunner(minProgress, progressCounter, Array[(Int, Double)]()) {

  var vertexSavePath: String = outputdir
  var edgeSavePath: String = outputdir
  override type R = Unit
  override type S = Unit

  /**
   * Save the graph at the given level of compression with community labels
   * level 0 = no compression
   *
   */
  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long]) = {
    vertexSavePath = outputdir + "/level_" + level + "_vertices"
    edgeSavePath = outputdir + "/level_" + level + "_edges"
    save(graph)
    qValues = qValues :+ ((level, q))
    println(s"qValue: $q")

    // overwrite the q values at each level
    sc.parallelize(qValues, 1).saveAsTextFile(outputdir + "/qvalues_" + level)
  }

  /**
   * Complete any final save actions required
   *
   */
  def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long]) = {

  }

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = {
    graph.vertices.saveAsTextFile(vertexSavePath)
    graph.edges.saveAsTextFile(edgeSavePath)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    kryo.writeObject(output, this.outputdir)

  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    this.outputdir = kryo.readObject(input, classOf[String])
  }
}