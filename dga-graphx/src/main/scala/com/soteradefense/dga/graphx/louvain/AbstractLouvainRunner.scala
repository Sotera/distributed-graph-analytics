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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.ObjectArraySerializer
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.soteradefense.dga.graphx.harness.Harness
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Abstract class for running the full louvain algorithm.
 * @param minimumCompressionProgress Minimum compression progress.
 * @param progressCounter Progress counter.
 */
abstract class AbstractLouvainRunner(var minimumCompressionProgress: Int, var progressCounter: Int, protected var qValues: Array[(Int, Double)]) extends Harness with Serializable with KryoSerializable {

  /**
   * Run method for running the full louvain algorithm.
   * @param sc The current spark context.
   * @param graph A Graph object with an optional edge weight.
   * @tparam VD ClassTag for the vertex data type.
   * @return The type of R
   */
  override def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): R = {
    val louvainCore = new LouvainCore
    var louvainGraph = louvainCore.createLouvainGraph(graph)

    var compressionLevel = -1 // number of times the graph has been compressed
    var q_modularityValue = -1.0 // current modularity value
    var halt = false
    do {
      compressionLevel += 1
      println(s"\nStarting Louvain level $compressionLevel")

      // label each vertex with its best community choice at this level of compression
      val (currentQModularityValue, currentGraph, numberOfPasses) = louvainCore.louvain(sc, louvainGraph, minimumCompressionProgress, progressCounter)
      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph
      
      // If modularity was increased by at least 0.001 compress the graph and repeat
      // halt immediately if the community labeling took less than 3 passes
      //println(s"if ($passes > 2 && $currentQ > $q + 0.001 )")
      //if (numberOfPasses > 2 && currentQModularityValue > q_modularityValue + 0.001) {
      if (currentQModularityValue > q_modularityValue + 0.001) {
        saveLevel(sc, compressionLevel, currentQModularityValue, louvainGraph)
        q_modularityValue = currentQModularityValue
        louvainGraph = louvainCore.compressGraph(louvainGraph)
      }
      else {
        halt = true
      }

    } while (!halt)
    finalSave(sc, compressionLevel, q_modularityValue, louvainGraph)
  }

  /**
   * Saves a single level of compression.
   * @param sc The current spark context.
   * @param level The level number.
   * @param q The q_value.
   * @param graph The compressed graph.
   */
  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long])

  /**
   * Abstract method for a final save after the algorithm is done.
   * @param sc The current SparkContext.
   * @param level Final Level.
   * @param q Final qvalues.
   * @param graph The final graph.
   * @return The type of R.
   */
  def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long]): R

  /**
   * Method for saving the louvain graph.
   * @param graph A graph of any type.
   * @tparam VD ClassTag for the vertex data.
   * @tparam ED ClassTag for the edge data.
   * @return The type of S
   */
  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.minimumCompressionProgress)
    kryo.writeObject(output, this.progressCounter)
    val objectArraySerializer = new ObjectArraySerializer
    objectArraySerializer.write(kryo, output, this.qValues.asInstanceOf[Array[Object]])
  }


  override def read(kryo: Kryo, input: Input): Unit = {
    this.minimumCompressionProgress = kryo.readObject(input, classOf[Int])
    this.progressCounter = kryo.readObject(input, classOf[Int])
    val objectArraySerializer = new ObjectArraySerializer
    this.qValues = objectArraySerializer.read(kryo, input, classOf[Array[Object]]).asInstanceOf[Array[(Int, Double)]]
  }
}