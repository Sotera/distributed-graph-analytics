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
package com.soteradefense.dga.graphx.hbse

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

import scala.collection.mutable

class HighBetweennessListComparator extends Ordering[(Long, Double)] {

  override def compare(x: (Long, Double), y: (Long, Double)): Int = {
    if (x._2 < y._2) -1 else if (x._2 > y._2) 1 else 0
  }

}

class HighBetweennessList(private var maxSize: Int, var betweennessQueue: mutable.PriorityQueue[(Long, Double)]) extends Serializable {

  def this(maxSize: Int) = this(maxSize, mutable.PriorityQueue[(Long, Double)]()(new HighBetweennessListComparator))

  def getMaxSize = this.maxSize

  def getBetweennessQueue = this.betweennessQueue

  def this() = this(1)

  def this(id: Long, betweenness: Double) = {
    this()
    this.betweennessQueue.enqueue((id, betweenness))
  }

  def this(maxSize: Int, id: Long, betweenness: Double) = {
    this(maxSize)
    this.betweennessQueue.enqueue((id, betweenness))
  }

  def highBetweennessSet: mutable.HashSet[Long] = {
    val set = new mutable.HashSet[Long]
    for (elem: (Long, Double) <- this.betweennessQueue)
      set.add(elem._1)
    set
  }
}

class HighBetweennessListSerializer extends Serializer[HighBetweennessList] {
  override def write(kryo: Kryo, output: Output, obj: HighBetweennessList): Unit = {
    kryo.writeObject(output, obj.getMaxSize)
    kryo.writeObject(output, obj.getBetweennessQueue.size)
    obj.getBetweennessQueue.foreach(f => {
      kryo.writeObject(output, f._1)
      kryo.writeObject(output, f._2)
    })
  }

  override def read(kryo: Kryo, input: Input, classType: Class[HighBetweennessList]): HighBetweennessList = {
    val maxSize = kryo.readObject(input, classOf[Int])
    val queueSize = kryo.readObject(input, classOf[Int])
    val priorityQueue = new mutable.PriorityQueue[(Long, Double)]
    var i = 0
    for(i <- 0 to (queueSize - 1)){
      priorityQueue.enqueue((kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Double])))
    }
    new HighBetweennessList(maxSize, priorityQueue)
  }
}
