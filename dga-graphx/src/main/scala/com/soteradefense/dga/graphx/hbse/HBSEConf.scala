package com.soteradefense.dga.graphx.hbse

import java.util.Date

import com.soteradefense.dga.hbse.HBSEConfigurationConstants
import org.apache.spark.SparkConf

class HBSEConf(var betweennessOutputDir: String,
               var shortestPathPhases: Int,
               var setStability: Int,
               var setStabilityCounter: Int,
               var betweennessSetMaxSize: Int,
               var pivotBatchSize: Int,
               var initialPivotBatchSize: Int,
               var pivotSelectionRandomSeed: Long,
               var vertexCount: Int,
               var defaultFileSystem: String) {

  def this() = this("", 1, 0, 1, 10, 5, 5, (new Date).getTime, 10, "")

  def this(sparkConf: SparkConf) = {
    this()
    this.betweennessOutputDir = sparkConf.get(HBSEConfigurationConstants.BETWEENNESS_OUTPUT_DIR, "")
    this.shortestPathPhases = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SHORTEST_PATH_PHASES, 5)
    this.setStability = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY, 0)
    this.setStabilityCounter = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY_COUNTER, 1)
    this.betweennessSetMaxSize = sparkConf.getInt(HBSEConfigurationConstants.BETWEENNESS_SET_MAX_SIZE, 10)
    this.pivotBatchSize = sparkConf.getInt(HBSEConfigurationConstants.PIVOT_BATCH_SIZE, 5)
    this.initialPivotBatchSize = sparkConf.getInt(HBSEConfigurationConstants.PIVOT_BATCH_SIZE_INITIAL, 5)
    this.pivotSelectionRandomSeed = sparkConf.getLong(HBSEConfigurationConstants.PIVOT_BATCH_RANDOM_SEED, (new Date).getTime)
    this.vertexCount = sparkConf.getInt(HBSEConfigurationConstants.VERTEX_COUNT, 10)
    this.defaultFileSystem = sparkConf.get(HBSEConfigurationConstants.FS_DEFAULT_FS, sparkConf.get(HBSEConfigurationConstants.FS_DEFAULT_NAME))
  }


  def increaseStabilityCounter() = this.setStabilityCounter += 1
}
