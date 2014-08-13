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

class PartialDependency(private var successors: Int, private var dependency: Double) extends Serializable {

  def this() = this(0, 0.0)

  def getDependency = this.dependency

  def getSuccessors = this.successors

  def setSuccessors(successors: Int) = this.successors = successors

  def setDependency(dep: Double) = this.dependency = dep

  def addSuccessors(diff: Int) = this.successors += diff

  def addDependency(diff: Double) = this.dependency += diff

}

class PartialDependencySerializer extends Serializer[PartialDependency] {
  override def write(kryo: Kryo, output: Output, obj: PartialDependency): Unit = {
    kryo.writeObject(output, obj.getSuccessors)
    kryo.writeObject(output, obj.getDependency)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[PartialDependency]): PartialDependency = {
    new PartialDependency(kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Double]))
  }
}