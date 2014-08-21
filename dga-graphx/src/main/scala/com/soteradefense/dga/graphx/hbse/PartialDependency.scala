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

/**
 * An object for encapsulating a partial dependency.
 *
 * @param numberOfSuccessors The number of successors for this dependency.
 * @param accumulatedDependency The dependency weight on this object.
 */
class PartialDependency(private var numberOfSuccessors: Int, private var accumulatedDependency: Double) extends Serializable with KryoSerializable {

  /**
   * Default constructor.
   * @return PartialDependency Object.
   */
  def this() = this(0, 0.0)

  /**
   * Return the dependency.
   * @return Dependency.
   */
  def getDependency = this.accumulatedDependency

  /**
   * Returns the number of successors.
   * @return Successors.
   */
  def getSuccessors = this.numberOfSuccessors

  /**
   * Set the number of successors.
   * @param successors value to set the successors to.
   */
  def setSuccessors(successors: Int) = this.numberOfSuccessors = successors

  /**
   * Set the dependency.
   * @param dependency value to set the dependency to.
   */
  def setDependency(dependency: Double) = this.accumulatedDependency = dependency

  /**
   * Accumulates the successor object.
   * @param valueToAdd value to add to the successors.
   */
  def accumulateSuccessors(valueToAdd: Int) = this.numberOfSuccessors += valueToAdd

  /**
   * Accumulates the dependency value.
   * @param valueToAdd value to add to the dependencies.
   */
  def accumulateDependency(valueToAdd: Double) = this.accumulatedDependency += valueToAdd

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.getSuccessors)
    kryo.writeObject(output, this.getDependency)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    this.numberOfSuccessors = kryo.readObject(input, classOf[Int])
    this.accumulatedDependency = kryo.readObject(input, classOf[Double])
  }
}