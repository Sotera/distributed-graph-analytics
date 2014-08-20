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
 * Object for storing the number of shortest paths through a node.
 * @param distance Shortest distance in the shortest path map.
 * @param predecessorPathCountMap Map for storing a nodes predecessors.
 */
class ShortestPathList(private var distance: Long, private var predecessorPathCountMap: mutable.HashMap[Long, Long]) extends Serializable with KryoSerializable {

  /**
   * Default Constructor.
   * @return ShortestPathList Object.
   */
  def this() = this(Long.MaxValue, new mutable.HashMap[Long, Long])

  /**
   * Constructor that accepts a path data object.
   * @param pathData PathData object to instantiate with.
   * @return ShortestPathList object.
   */
  def this(pathData: PathData) = {
    this()
    this.distance = pathData.getDistance
    this.predecessorPathCountMap.put(pathData.getMessageSource, pathData.getNumberOfShortestPaths)
  }

  /**
   * Returns the predecessorPathCountMap.
   * @return value of predecessorPathCountMap.
   */
  def getPredecessorPathCountMap = this.predecessorPathCountMap

  /**
   * Returns the distance of the shortestpathlist.
   * @return value of distance.
   */
  def getDistance = this.distance

  /**
   * Gets the shortest path count.
   * @return Accumulated total of the shortest path count.
   */
  def getShortestPathCount = this.predecessorPathCountMap.foldLeft(0L)((total: Long, mapItem: (Long, Long)) => total + mapItem._2)


  def update(pathData: PathData): Boolean = {
    var updated: Boolean = false
    if (this.distance == pathData.getDistance) {
      val oldNumberOfShortestPaths = this.predecessorPathCountMap.getOrElse(pathData.getMessageSource, Long.MinValue)
      updated = oldNumberOfShortestPaths != pathData.getNumberOfShortestPaths
      if (updated) {
        this.predecessorPathCountMap.put(pathData.getMessageSource, pathData.getNumberOfShortestPaths)
      }
    } else if (pathData.getDistance < this.distance) {
      this.distance = pathData.getDistance
      this.predecessorPathCountMap.clear()
      this.predecessorPathCountMap.put(pathData.getMessageSource, pathData.getNumberOfShortestPaths)
      updated = true
    } else {
      updated = false
    }
    updated
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.getDistance)
    kryo.writeObject(output, this.getPredecessorPathCountMap.size)
    this.getPredecessorPathCountMap.foreach(f => {
      val (vertexId, numberOfShortestPaths) = f
      kryo.writeObject(output, vertexId)
      kryo.writeObject(output, numberOfShortestPaths)
    })
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val dist = kryo.readObject(input, classOf[Long])
    val pathCountMapSize = kryo.readObject(input, classOf[Int])
    val map = new mutable.HashMap[Long, Long]
    var i = 0
    for (i <- 0 to pathCountMapSize) {
      map.put(kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]))
    }
    this.distance = dist
    this.predecessorPathCountMap = map
  }
}