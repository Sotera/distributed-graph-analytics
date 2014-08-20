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

package com.soteradefense.dga.graphx.pr

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Output, Input}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class PRTestRunner(delta: Double) extends AbstractPRRunner(delta) {
  override type S = Graph[Double, Long]

  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S = graph.asInstanceOf[Graph[Double, Long]]

  override def write(kryo: Kryo, output: Output): Unit = super.write(kryo, output)

  override def read(kryo: Kryo, input: Input): Unit = super.read(kryo, input)
}
