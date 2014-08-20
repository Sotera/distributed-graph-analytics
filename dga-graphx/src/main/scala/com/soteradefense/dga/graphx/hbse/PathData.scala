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
import com.esotericsoftware.kryo.{KryoSerializable, Kryo, Serializer}

/**
 * PathData object for storing path data from a pivot and through a node.
 *
 * @param distance Distance from the pivot to message source.
 * @param pivotSource Source of the initial message.
 * @param messageSource Source of the node that forwarded that message to you.
 * @param numPaths Number of paths it took to get there.
 */
class PathData(private var distance: Long, private var pivotSource: Long, private var messageSource: Long, private var numPaths: Long) extends Serializable with KryoSerializable {
  /**
   * Returns the distance value.
   * @return value of distance.
   */
  def getDistance = this.distance

  /**
   * Returns the pivot who send the initial message.
   * @return value of pivotSource.
   */
  def getPivotSource = this.pivotSource

  /**
   * Returns the node that forwarded the message.
   * @return value of messageSource.
   */
  def getMessageSource = this.messageSource

  /**
   * Returns the number of shortest paths for this object.
   * @return value of numPaths.
   */
  def getNumberOfShortestPaths = this.numPaths

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.getDistance)
    kryo.writeObject(output, this.getPivotSource)
    kryo.writeObject(output, this.getMessageSource)
    kryo.writeObject(output, this.getNumberOfShortestPaths)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    this.distance = kryo.readObject(input, classOf[Long])
    this.pivotSource = kryo.readObject(input, classOf[Long])
    this.messageSource = kryo.readObject(input, classOf[Long])
    this.numPaths = kryo.readObject(input, classOf[Long])
  }
}

/**
 * Helper object for instantiating a PathData object.
 */
object PathData {
  /**
   * Creates a PathData object intended for the shortest path run.
   * @param pivotSource Pivot Source.
   * @param messageSource Who sent the message.
   * @param distance Distance from pivot source to message source.
   * @param numPaths Number of Shortest paths.
   * @return
   */
  def createShortestPathMessage(pivotSource: Long, messageSource: Long, distance: Long, numPaths: Long) = new PathData(distance, pivotSource, messageSource, numPaths)
}