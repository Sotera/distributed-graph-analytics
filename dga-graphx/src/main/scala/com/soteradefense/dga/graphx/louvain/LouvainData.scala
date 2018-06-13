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
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class LouvainData(var name: String, var community: Long, var communityName: String, var communitySigmaTot: Long, var internalWeight: Long, var nodeWeight: Long, var changed: Boolean) extends Serializable with KryoSerializable {

  def this() = this(null, -1L, null, 0L, 0L, 0L, false)


  override def toString: String = s"{name:$name,community:$community,communityName:$communityName,communitySigmaTot:$communitySigmaTot,internalWeight:$internalWeight,nodeWeight:$nodeWeight}"


  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.name)
    kryo.writeObject(output, this.community)
    kryo.writeObject(output, this.communityName)
    kryo.writeObject(output, this.communitySigmaTot)
    kryo.writeObject(output, this.internalWeight)
    kryo.writeObject(output, this.nodeWeight)
    kryo.writeObject(output, this.changed)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    this.name = kryo.readObject(input, classOf[String])
    this.community = kryo.readObject(input, classOf[Long])
    this.communityName = kryo.readObject(input, classOf[String])
    this.communitySigmaTot = kryo.readObject(input, classOf[Long])
    this.internalWeight = kryo.readObject(input, classOf[Long])
    this.nodeWeight = kryo.readObject(input, classOf[Long])
    this.changed = kryo.readObject(input, classOf[Boolean])
  }
}