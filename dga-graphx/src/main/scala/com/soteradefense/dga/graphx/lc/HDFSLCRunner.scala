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
package com.soteradefense.dga.graphx.lc

import com.esotericsoftware.kryo.Serializer
import com.twitter.chill._
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag


class HDFSLCRunner(var output_dir: String, var delimiter: String) extends AbstractLCRunner {

  override type S = Unit

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = {
    graph.triplets.map(t => s"${t.srcId}$delimiter${t.dstId}").saveAsTextFile(output_dir)
  }
}

class HDFSLCRunnerSerializer extends Serializer[HDFSLCRunner] {
  override def write(kryo: Kryo, out: Output, obj: HDFSLCRunner): Unit = {
    kryo.writeObject(out, obj.output_dir)
    kryo.writeObject(out, obj.delimiter)
  }

  override def read(kryo: Kryo, in: Input, cls: Class[HDFSLCRunner]): HDFSLCRunner = {
    new HDFSLCRunner(kryo.readObject(in, classOf[String]), kryo.readObject(in, classOf[String]))
  }
}