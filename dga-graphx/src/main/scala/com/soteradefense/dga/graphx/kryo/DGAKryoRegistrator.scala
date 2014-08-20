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
import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import com.soteradefense.dga.graphx.lc.HDFSLCRunner
import com.soteradefense.dga.graphx.louvain.{HDFSLouvainRunner, LouvainData}
import com.soteradefense.dga.graphx.pr.HDFSPRRunner
import com.soteradefense.dga.graphx.wcc.HDFSWCCRunner
import org.apache.spark.graphx.GraphKryoRegistrator

/**
 * Kryo Registrator for DGA's classes.  Extends from the GraphX Registrator.
 */
class DGAKryoRegistrator extends GraphKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)
    kryo.register(classOf[EdgeInputFormat])

    //WCC
    kryo.register(classOf[HDFSWCCRunner])

    //HBSE
    kryo.register(classOf[PartialDependency])
    kryo.register(classOf[PathData])
    kryo.register(classOf[HBSEData])
    kryo.register(classOf[ShortestPathList])
    kryo.register(classOf[HDFSHBSERunner])
    kryo.register(classOf[HBSEConf])

    //LC
    kryo.register(classOf[HDFSLCRunner])

    //PR
    kryo.register(classOf[HDFSPRRunner])

    //Louvain
    kryo.register(classOf[HDFSLouvainRunner])
    kryo.register(classOf[LouvainData])
  }
}
