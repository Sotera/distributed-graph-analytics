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
import com.esotericsoftware.kryo.{Kryo, Serializer}

import scala.collection.mutable

/**
 * Object that stores the required data for the HBSE Run.
 *
 * @param pathDataMap A hash map of accumulated PathData from the Pivots.
 * @param partialDepMap A hash map of dependency accumulation.
 * @param approximateBetweenness Approximate Betweenness value.
 */
class HBSEData(private var pathDataMap: mutable.HashMap[Long, ShortestPathList], private var partialDepMap: mutable.HashMap[Long, PartialDependency], private var approximateBetweenness: Double) extends Serializable {

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
    if (!pathDataMap.contains(source)) {
      list = new ShortestPathList(pathData)
      this.pathDataMap.put(source, list)
    }
    else {
      list = pathDataMap.get(source).get
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
   * @return pathDataMap
   */
  def getPathDataMap = this.pathDataMap

  /**
   * Adds or updates a partial dependency to the hash map.
   * @param src Node that has the dependency.
   * @param dependency Value to add to the map.
   * @param numberOfSuccessors numberOfSuccessors for this node.
   * @return The partial dependency object that was updated.
   */
  def addPartialDependency(src: Long, dependency: Double, numberOfSuccessors: Int): PartialDependency = {
    val current: PartialDependency = this.partialDepMap.getOrElse(src, new PartialDependency)
    current.accumulateDependency(dependency)
    current.accumulateSuccessors(numberOfSuccessors)
    this.partialDepMap.put(src, current)
    current
  }

  /**
   * Returns the partial dependency map.
   * @return value of partialDepMap.
   */
  def getPartialDependencyMap = this.partialDepMap

}

/**
 * Kryo serializer for the HBSEData object.
 */
class HBSEDataSerializer extends Serializer[HBSEData] {

  override def write(kryo: Kryo, output: Output, obj: HBSEData): Unit = {
    kryo.writeObject(output, obj.getPathDataMap.size)
    obj.getPathDataMap.foreach(f => {
      kryo.writeObject(output, f._1)
      val serializer = new ShortestPathListSerializer
      serializer.write(kryo, output, f._2)
    })

    kryo.writeObject(output, obj.getPartialDependencyMap.size)
    obj.getPartialDependencyMap.foreach(f => {
      kryo.writeObject(output, f._1)
      val serializer = new PartialDependencySerializer
      serializer.write(kryo, output, f._2)
    })

    kryo.writeObject(output, obj.getApproximateBetweenness)
  }

  override def read(kryo: Kryo, input: Input, classType: Class[HBSEData]): HBSEData = {
    val pdSize = kryo.readObject(input, classOf[Int])
    var i = 0
    val pathDataMap = new mutable.HashMap[Long, ShortestPathList]
    for (i <- 0 to (pdSize - 1)) {
      val serializer = new ShortestPathListSerializer
      pathDataMap.put(kryo.readObject(input, classOf[Long]), serializer.read(kryo, input, classOf[ShortestPathList]))
    }
    i = 0
    val partialDepSize = kryo.readObject(input, classOf[Int])
    val partialDepMap = new mutable.HashMap[Long, PartialDependency]
    for (i <- 0 to (partialDepSize - 1)) {
      val serializer = new PartialDependencySerializer
      partialDepMap.put(kryo.readObject(input, classOf[Long]), serializer.read(kryo, input, classOf[PartialDependency]))
    }
    new HBSEData(pathDataMap, partialDepMap, kryo.readObject(input, classOf[Double]))
  }
}