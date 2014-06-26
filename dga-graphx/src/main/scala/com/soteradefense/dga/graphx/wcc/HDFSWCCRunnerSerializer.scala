package com.soteradefense.dga.graphx.wcc

import com.twitter.chill.{Input, Kryo, ObjectSerializer, Output}


class HDFSWCCRunnerSerializer extends ObjectSerializer[HDFSWCCRunner] {
  override def write(kser: Kryo, out: Output, obj: HDFSWCCRunner): Unit = {
    super.write(kser, out, obj)
    kser.writeObject(out, obj.output_dir)
    kser.writeObject(out, obj.delimiter)
  }

  override def read(kser: Kryo, in: Input, cls: Class[HDFSWCCRunner]): HDFSWCCRunner = {
    super.read(kser, in, cls)
    new HDFSWCCRunner(kser.readObject(in, classOf[String]), kser.readObject(in, classOf[String]))
  }
}
