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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import scala.collection.mutable

/**
 * Object that stores the required data for the HBSE Run.
 *
 * @param pivotPathMap A hash map of accumulated PathData from the Pivots.
 * @param partialDependencyMap A hash map of dependency accumulation.
 * @param approximateBetweenness Approximate Betweenness value.
 */
class HBSEData(private var pivotPathMap: mutable.HashMap[Long, ShortestPathList], private var partialDependencyMap: mutable.HashMap[Long, PartialDependency], private var approximateBetweenness: Double)
  extends Serializable with KryoSerializable {

  /**
   * Constructor that accepts an initial betweenness value.
   * @param betweenness Betweenness value to instantiate.
   * @return HBSEData object.
   */
  def this(betweenness: Double) = this(new mutable.HashMap[Long, ShortestPathList], new mutable.HashMap[Long, PartialDependency], betweenness)

  /**
   * Default Constructor
   * @return HBSEData object.
   */
  def this() = this(0.0)


  /**
   * Add or updates path data in the path data map.
   * @param pathData The new path data.
   * @return A shortest path list if it was updated or else null.
   */
  def addPathData(pathData: PathData): ShortestPathList = {
    var list: ShortestPathList = null
    val source: Long = pathData.getPivotSource
    if (!pivotPathMap.contains(source)) {
      list = new ShortestPathList(pathData)
      this.pivotPathMap.put(source, list)
    }
    else {
      list = pivotPathMap.get(source).get
      list = if (list.update(pathData)) list else null
    }
    list
  }

  /**
   * Return the approxBetweenness value.
   * @return value of approxBetweenness.
   */
  def getApproximateBetweenness = this.approximateBetweenness


  /**
   * Returns the path data map.
   * @return pivotPathMap
   */
  def getPathDataMap = this.pivotPathMap

  /**
   * Adds or updates a partial dependency to the hash map.
   * @param src Node that has the dependency.
   * @param dependency Value to add to the map.
   * @param numberOfSuccessors numberOfSuccessors for this node.
   * @return The partial dependency object that was updated.
   */
  def addPartialDependency(src: Long, dependency: Double, numberOfSuccessors: Int): PartialDependency = {
    val current: PartialDependency = this.partialDependencyMap.getOrElse(src, new PartialDependency)
    current.accumulateDependency(dependency)
    current.accumulateSuccessors(numberOfSuccessors)
    this.partialDependencyMap.put(src, current)
    current
  }

  /**
   * Returns the partial dependency map.
   * @return value of partialDependencyMap.
   */
  def getPartialDependencyMap = this.partialDependencyMap

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.getPathDataMap.size)
    this.getPathDataMap.foreach(f => {
      val (pivotId, shortestPathList) = f
      kryo.writeObject(output, pivotId)
      shortestPathList.write(kryo, output)
    })

    kryo.writeObject(output, this.getPartialDependencyMap.size)
    this.getPartialDependencyMap.foreach(f => {
      val (vertexId, partialDependency) = f
      kryo.writeObject(output, vertexId)
      partialDependency.write(kryo, output)
    })

    kryo.writeObject(output, this.getApproximateBetweenness)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val pdSize = kryo.readObject(input, classOf[Int])
    var i = 0
    val pdMap = new mutable.HashMap[Long, ShortestPathList]
    for (i <- 0 to (pdSize - 1)) {
      val shortestPathList = new ShortestPathList
      val pivotId = kryo.readObject(input, classOf[Long])
      shortestPathList.read(kryo, input)
      pdMap.put(pivotId, shortestPathList)
    }

    i = 0
    val partialDepSize = kryo.readObject(input, classOf[Int])
    val partialDMap = new mutable.HashMap[Long, PartialDependency]
    for (i <- 0 to (partialDepSize - 1)) {
      val partialDep = new PartialDependency
      partialDep.read(kryo, input)
    }
    this.pivotPathMap = pdMap
    this.partialDependencyMap = partialDMap
    this.approximateBetweenness = kryo.readObject(input, classOf[Double])
  }
}
