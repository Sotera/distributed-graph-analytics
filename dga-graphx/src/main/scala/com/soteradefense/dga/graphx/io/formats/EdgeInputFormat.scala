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
package com.soteradefense.dga.graphx.io.formats

import com.esotericsoftware.kryo.KryoSerializable
import com.twitter.chill._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


/**
 * Input format for reading an edge lists into an RDD of Edges
 * @param inputFile Location of the edge list.
 * @param delimiter Delimiter that splits the edges.
 * @param parallelism The number of tasks to do in parallel.
 */
class EdgeInputFormat(var inputFile: String, var delimiter: String, var parallelism: Int = 20) extends Serializable with KryoSerializable  {
  /**
   * Reads in an edge list from the class variable inputFile
   * @param sc The spark context to use to read in the file.
   * @param typeConversionMethod A method that is passed in to convert your data to a long value.
   * @return Edge RDD.
   */
  def getEdgeRDD(sc: SparkContext, typeConversionMethod: String => Long = _.toLong): RDD[Edge[Long]] = {
    sc.textFile(inputFile, parallelism).map(row => {
      val tokens = row.split(delimiter).map(_.trim())
      tokens.length match {
        case 2 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), 1L)
        case 3 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), tokens(2).toLong)
        case _ => throw new IllegalArgumentException("invalid input line: " + row)
      }
    })
  }
  
  /**
   * Reads in an edge list from the class variable inputFile containing edge list using String ids
   * @param sc The spark context to use to read in the file.
   * @param 
   * @return Graph.
   */  
  def getGraphFromStringEdgeList(sc: SparkContext): Graph[String, Long] = {
    val rdd = sc.textFile(inputFile, parallelism).map(row => {
      val tokens = row.split(delimiter).map(_.trim())
      tokens.length match {
        case 2 => (tokens(0), tokens(1), 1L)
        case 3 => (tokens(0), tokens(1), tokens(2).toLong)
        case _ => throw new IllegalArgumentException("invalid input line: " + row)
      }
    }).cache()
    
    val distinctNodes = rdd.flatMap({case (src, dst, weight) => Seq((src, 0),(dst, 0)) })
      .reduceByKey(_+_)
      .map({case (name,count) => name})
      .cache()
    
    // Invoke count to materialize the distinctNodes rdd
    println("*** found " + distinctNodes.count() + " nodes")
    
    val nodesWithId = distinctNodes.zipWithUniqueId()
    val nodeIdMap = nodesWithId.collectAsMap
    val edges = rdd.map( x =>
      Edge(nodeIdMap.get(x._1).get, nodeIdMap.get(x._2).get, x._3.asInstanceOf[Long])
    ).cache()
    val vertices = nodesWithId.map({case (name, id)=> (id, name)}).cache()
    
    rdd.unpersist(blocking = false)
    distinctNodes.unpersist(blocking = false)
    
    Graph(vertices,edges).partitionBy(PartitionStrategy.EdgePartition2D)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.inputFile)
    kryo.writeObject(output, this.delimiter)
    kryo.writeObject(output, this.parallelism)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    this.inputFile = kryo.readObject(input, classOf[String])
    this.delimiter = kryo.readObject(input, classOf[String])
    this.parallelism = kryo.readObject(input, classOf[Int])
  }
}