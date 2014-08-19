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

import java.util.Date

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.soteradefense.dga.hbse.HBSEConfigurationConstants
import org.apache.spark.SparkConf

/**
 * Stores the configuration values for the HighBetweennessCore Object.
 *
 * @param shortestPathPhases The number of shortest path phases to run through.
 * @param setStability The delta value that must be met for the set to be stable.
 * @param setStabilityCounter The number of times the set stability counter must be met.
 * @param betweennessSetMaxSize The size of the betweenness set.
 * @param pivotBatchSize The number of pivots to select per run.
 * @param initialPivotBatchSize The number of pivots to use in the first run.
 * @param pivotSelectionRandomSeed The random seed to pass to the takeSample.
 * @param totalNumberOfPivots The total number of pivots to use in an entire run.
 */
class HBSEConf(
                var shortestPathPhases: Int,
                var setStability: Int,
                var setStabilityCounter: Int,
                var betweennessSetMaxSize: Int,
                var pivotBatchSize: Int,
                var initialPivotBatchSize: Int,
                var pivotSelectionRandomSeed: Long,
                var totalNumberOfPivots: Int) extends Serializable {

  /**
   * The default constructor for an HBSEConf Object.
   * @return HBSEConf object with defaults.
   */
  def this() = this(1, 0, 1, 10, 5, 5, (new Date).getTime, 10)

  /**
   * Constructor that instantiates an HBSEConf object from SparkConf values.
   * @param sparkConf SparkConf object.
   */
  def this(sparkConf: SparkConf) = {
    this()
    this.shortestPathPhases = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SHORTEST_PATH_PHASES, 1)
    this.setStability = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY, 0)
    this.setStabilityCounter = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY_COUNTER, 5)
    this.betweennessSetMaxSize = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SET_MAX_SIZE, 10)
    this.pivotBatchSize = sparkConf.getInt(HBSEConfigurationConstants.PIVOT_BATCH_SIZE, 5)
    this.initialPivotBatchSize = sparkConf.getInt(HBSEConfigurationConstants.PIVOT_BATCH_SIZE_INITIAL, 5)
    this.pivotSelectionRandomSeed = sparkConf.getLong(HBSEConfigurationConstants.PIVOT_BATCH_RANDOM_SEED, (new Date).getTime)
    this.totalNumberOfPivots = sparkConf.getInt(HBSEConfigurationConstants.TOTAL_PIVOT_COUNT, 10)
  }

}

/**
 * Kryo Serializer for the HBSEConf Object
 */
class HBSEConfSerializer extends Serializer[HBSEConf] {
  override def write(kryo: Kryo, output: Output, obj: HBSEConf): Unit = {
    kryo.writeObject(output, obj.shortestPathPhases)
    kryo.writeObject(output, obj.setStability)
    kryo.writeObject(output, obj.setStabilityCounter)
    kryo.writeObject(output, obj.betweennessSetMaxSize)
    kryo.writeObject(output, obj.pivotBatchSize)
    kryo.writeObject(output, obj.initialPivotBatchSize)
    kryo.writeObject(output, obj.pivotSelectionRandomSeed)
    kryo.writeObject(output, obj.totalNumberOfPivots)
  }

  override def read(kryo: Kryo, input: Input, classType: Class[HBSEConf]): HBSEConf = {
    new HBSEConf(kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Int]),
      kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Int]))
  }
}
