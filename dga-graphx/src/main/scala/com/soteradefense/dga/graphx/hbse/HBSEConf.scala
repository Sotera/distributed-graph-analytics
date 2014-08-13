package com.soteradefense.dga.graphx.hbse

import java.util.Date

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.soteradefense.dga.hbse.HBSEConfigurationConstants
import org.apache.spark.SparkConf

class HBSEConf(
                var shortestPathPhases: Int,
                var setStability: Int,
                var setStabilityCounter: Int,
                var betweennessSetMaxSize: Int,
                var pivotBatchSize: Int,
                var initialPivotBatchSize: Int,
                var pivotSelectionRandomSeed: Long,
                var vertexCount: Int) extends Serializable {

  def this() = this(1, 0, 1, 10, 5, 5, (new Date).getTime, 10)

  def this(sparkConf: SparkConf) = {
    this()
    this.shortestPathPhases = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SHORTEST_PATH_PHASES, 1)
    this.setStability = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY, 0)
    this.setStabilityCounter = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY_COUNTER, 5)
    this.betweennessSetMaxSize = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SET_MAX_SIZE, 10)
    this.pivotBatchSize = sparkConf.getInt(HBSEConfigurationConstants.PIVOT_BATCH_SIZE, 5)
    this.initialPivotBatchSize = sparkConf.getInt(HBSEConfigurationConstants.PIVOT_BATCH_SIZE_INITIAL, 5)
    this.pivotSelectionRandomSeed = sparkConf.getLong(HBSEConfigurationConstants.PIVOT_BATCH_RANDOM_SEED, (new Date).getTime)
    this.vertexCount = sparkConf.getInt(HBSEConfigurationConstants.VERTEX_COUNT, 10)
  }


  def increaseStabilityCounter() = this.setStabilityCounter += 1
}

class HBSEConfSerializer extends Serializer[HBSEConf] {
  override def write(kryo: Kryo, output: Output, obj: HBSEConf): Unit = {
    kryo.writeObject(output, obj.shortestPathPhases)
    kryo.writeObject(output, obj.setStability)
    kryo.writeObject(output, obj.setStabilityCounter)
    kryo.writeObject(output, obj.betweennessSetMaxSize)
    kryo.writeObject(output, obj.pivotBatchSize)
    kryo.writeObject(output, obj.initialPivotBatchSize)
    kryo.writeObject(output, obj.pivotSelectionRandomSeed)
    kryo.writeObject(output, obj.vertexCount)
  }

  override def read(kryo: Kryo, input: Input, classType: Class[HBSEConf]): HBSEConf = {
    new HBSEConf(kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Int]),
      kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Int]))
  }
}
