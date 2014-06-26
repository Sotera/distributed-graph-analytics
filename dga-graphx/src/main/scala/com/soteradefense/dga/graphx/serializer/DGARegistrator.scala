package com.soteradefense.dga.graphx.serializer

import com.esotericsoftware.kryo.Kryo
import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import org.apache.spark.graphx.GraphKryoRegistrator
import org.apache.spark.serializer.KryoRegistrator

class DGARegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    //kryo.register(classOf[HDFSWCCRunner], new HDFSWCCRunnerSerializer())
    kryo.register(classOf[EdgeInputFormat])
    //kryo.register(classOf[WeaklyConnectedComponentsHarness])
    val graphXReg = new GraphKryoRegistrator()
    graphXReg.registerClasses(kryo)
  }
}
