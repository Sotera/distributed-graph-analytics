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
 package com.soteradefense.dga.graphx.neighboringcommunity.louvain

import com.soteradefense.dga.graphx.louvain.{AbstractLouvainRunner, LouvainData}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.ClassTag

class NeighboringCommunityLouvainRunner(minProgress: Int, progressCounter: Int) extends AbstractLouvainRunner(minProgress, progressCounter, Array[(Int, Double)]()) {
  var communityMapping: RDD[(VertexId, Long)] = null

  override def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long]): Unit = {
    if (communityMapping != null) {
      val levelRDD = graph.vertices.map({ case (id, v) => (id, v.community)})
      val communityMappingPair = new PairRDDFunctions[Long, VertexId](communityMapping.map({ case (id, community) => (community, id)}))
      val join = communityMappingPair.leftOuterJoin(levelRDD)
        .map({ case (prevCommunity, (actualId, currentCommunityOption)) =>
        (actualId, currentCommunityOption.getOrElse(prevCommunity))
      }).cache()
      join.count()
      communityMapping.unpersist()
      communityMapping = join
    } else {
      communityMapping = graph.vertices.map({ case (id, v) => (id, v.community)}).cache()
    }
  }

  override def save[VD, ED](graph: Graph[VD, ED])(implicit evidence$2: ClassTag[VD], evidence$3: ClassTag[ED]): S = communityMapping

  override def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long]): R = communityMapping

  override type S = RDD[(VertexId, Long)]
  override type R = S
}
