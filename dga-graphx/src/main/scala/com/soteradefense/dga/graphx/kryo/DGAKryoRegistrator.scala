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
package com.soteradefense.dga.graphx.kryo

import com.esotericsoftware.kryo.Kryo
import com.soteradefense.dga.graphx.hbse._
import com.soteradefense.dga.graphx.io.formats.{EdgeInputFormat, EdgeInputFormatSerializer}
import com.soteradefense.dga.graphx.lc.{HDFSLCRunnerSerializer, HDFSLCRunner}
import com.soteradefense.dga.graphx.louvain.{VertexStateSerializer, VertexState, HDFSLouvainRunnerSerializer, HDFSLouvainRunner}
import com.soteradefense.dga.graphx.pr.{HDFSPRRunnerSerializer, HDFSPRRunner}
import com.soteradefense.dga.graphx.wcc.{HDFSWCCRunner, HDFSWCCRunnerSerializer}
import org.apache.spark.graphx.GraphKryoRegistrator

import scala.collection.immutable

class DGAKryoRegistrator extends GraphKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)
    kryo.register(classOf[EdgeInputFormat], new EdgeInputFormatSerializer)

    //WCC
    kryo.register(classOf[HDFSWCCRunner], new HDFSWCCRunnerSerializer)

    //HBSE
    kryo.register(classOf[PartialDependency], new PartialDependencySerializer)
    kryo.register(classOf[PathData], new PathDataSerializer)
    kryo.register(classOf[HBSEData], new HBSEDataSerializer)
    kryo.register(classOf[ShortestPathList], new ShortestPathListSerializer)
    kryo.register(classOf[HDFSHBSERunner], new HDFSHBSERunnerSerializer)
    kryo.register(classOf[HBSEConf], new HBSEConfSerializer)

    //LC
    kryo.register(classOf[HDFSLCRunner], new HDFSLCRunnerSerializer)

    //PR
    kryo.register(classOf[HDFSPRRunner], new HDFSPRRunnerSerializer)

    //Louvain
    kryo.register(classOf[HDFSLouvainRunner], new HDFSLouvainRunnerSerializer)
    kryo.register(classOf[VertexState], new VertexStateSerializer)
  }
}
