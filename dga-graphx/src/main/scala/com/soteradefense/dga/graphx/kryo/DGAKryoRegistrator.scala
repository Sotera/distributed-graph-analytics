package com.soteradefense.dga.graphx.kryo

import com.esotericsoftware.kryo.Kryo
import com.soteradefense.dga.graphx.io.formats.{EdgeInputFormat, EdgeInputFormatSerializer}
import com.soteradefense.dga.graphx.wcc.{HDFSWCCRunner, HDFSWCCRunnerSerializer}
import org.apache.spark.graphx.GraphKryoRegistrator

class DGAKryoRegistrator extends GraphKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)
    kryo.register(classOf[HDFSWCCRunner], new HDFSWCCRunnerSerializer)
    kryo.register(classOf[EdgeInputFormat], new EdgeInputFormatSerializer)
  }
}
