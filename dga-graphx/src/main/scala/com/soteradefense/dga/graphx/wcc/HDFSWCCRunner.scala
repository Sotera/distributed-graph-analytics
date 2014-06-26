package com.soteradefense.dga.graphx.wcc

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.DefaultSerializers
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class HDFSWCCRunner(var output_dir: String, var delimiter: String) extends WeaklyConnectedComponentsHarness {
  val serializer = new DefaultSerializers.StringSerializer
  override def save[VD: ClassTag](graph: Graph[VD, Long]): Unit = {
    graph.triplets.map(t => s"${t.srcId}$delimiter${t.dstId}$delimiter${t.srcAttr}").saveAsTextFile(output_dir)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    serializer.write(kryo, output, output_dir)
    serializer.write(kryo, output, delimiter)
//    kryo.writeObject(output, output_dir)
//    kryo.writeObject(output, delimiter)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    this.output_dir = serializer.read(kryo, input, classOf[String])
    this.delimiter = serializer.read(kryo, input, classOf[String])

//    this.output_dir = kryo.readObject(input, classOf[String])
//    this.delimiter = kryo.readObject(input, classOf[String])
  }
}