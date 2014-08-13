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
import com.esotericsoftware.kryo.{Kryo, Serializer}

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState(var community: Long, var communitySigmaTot: Long, var internalWeight: Long, var nodeWeight: Long, var changed: Boolean) extends Serializable {

  def this() {
    this(-1L, 0L, 0L, 0L, false)
  }

  override def toString: String = {
    "{community:" + community + ",communitySigmaTot:" + communitySigmaTot +
      ",internalWeight:" + internalWeight + ",nodeWeight:" + nodeWeight + "}"
  }
}

class VertexStateSerializer extends Serializer[VertexState] {
  override def write(kryo: Kryo, output: Output, obj: VertexState): Unit = {
    kryo.writeObject(output, obj.community)
    kryo.writeObject(output, obj.communitySigmaTot)
    kryo.writeObject(output, obj.internalWeight)
    kryo.writeObject(output, obj.nodeWeight)
    kryo.writeObject(output, obj.changed)
  }

  override def read(kryo: Kryo, input: Input, classType: Class[VertexState]): VertexState = {
    new VertexState(kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]), kryo.readObject(input,
      classOf[Boolean]))
  }
}