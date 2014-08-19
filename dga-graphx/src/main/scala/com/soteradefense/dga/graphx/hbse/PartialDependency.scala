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

/**
 * An object for encapsulating a partial dependency.
 *
 * @param successors The number of successors for this dependency.
 * @param dependency The dependency weight on this object.
 */
class PartialDependency(private var successors: Int, private var dependency: Double) extends Serializable {

  /**
   * Default constructor.
   * @return PartialDependency Object.
   */
  def this() = this(0, 0.0)

  /**
   * Return the dependency.
   * @return Dependency.
   */
  def getDependency = this.dependency

  /**
   * Returns the number of successors.
   * @return Successors.
   */
  def getSuccessors = this.successors

  /**
   * Set the number of successors.
   * @param successors value to set the successors to.
   */
  def setSuccessors(successors: Int) = this.successors = successors

  /**
   * Set the dependency.
   * @param dep value to set the dependency to.
   */
  def setDependency(dep: Double) = this.dependency = dep

  /**
   * Accumulates the successor object.
   * @param diff value to add to the successors.
   */
  def accumulateSuccessors(diff: Int) = this.successors += diff

  /**
   * Accumulates the dependency value.
   * @param diff value to add to the dependencies.
   */
  def accumulateDependency(diff: Double) = this.dependency += diff

}

/**
 * Kryo Serializer for the PartialDependency Object.
 */
class PartialDependencySerializer extends Serializer[PartialDependency] {
  override def write(kryo: Kryo, output: Output, obj: PartialDependency): Unit = {
    kryo.writeObject(output, obj.getSuccessors)
    kryo.writeObject(output, obj.getDependency)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[PartialDependency]): PartialDependency = {
    new PartialDependency(kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Double]))
  }
}