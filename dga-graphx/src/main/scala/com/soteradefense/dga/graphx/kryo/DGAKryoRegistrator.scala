package com.soteradefense.dga.graphx.kryo

import com.esotericsoftware.kryo.Kryo
import com.soteradefense.dga.graphx.hbse._
import com.soteradefense.dga.graphx.io.formats.{EdgeInputFormat, EdgeInputFormatSerializer}
import com.soteradefense.dga.graphx.lc.{HDFSLCRunner, HDFSLCRunnerSerializer}
import com.soteradefense.dga.graphx.louvain._
import com.soteradefense.dga.graphx.pr.{HDFSPRRunner, HDFSPRRunnerSerializer}
import com.soteradefense.dga.graphx.wcc.{HDFSWCCRunner, HDFSWCCRunnerSerializer}
import org.apache.spark.graphx.GraphKryoRegistrator

class DGAKryoRegistrator extends GraphKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)
    kryo.register(classOf[EdgeInputFormat], new EdgeInputFormatSerializer)

    //WCC
    kryo.register(classOf[HDFSWCCRunner], new HDFSWCCRunnerSerializer)
    kryo.register(classOf[HDFSWCCRunner], new HDFSWCCRunnerSerializer)

    //HBSE
    kryo.register(classOf[HighBetweennessList], new HighBetweennessListSerializer)
    kryo.register(classOf[PartialDependency], new PartialDependencySerializer)
    kryo.register(classOf[PathData], new PathDataSerializer)
    kryo.register(classOf[VertexData], new VertexDataSerializer)
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
