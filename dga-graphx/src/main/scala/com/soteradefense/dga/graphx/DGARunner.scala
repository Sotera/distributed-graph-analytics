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
package com.soteradefense.dga.graphx

import java.util.Date

import com.soteradefense.dga.graphx.config.Config
import com.soteradefense.dga.graphx.harness.Harness
import com.soteradefense.dga.graphx.hbse.HDFSHBSERunner
import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import com.soteradefense.dga.graphx.lc.HDFSLCRunner
import com.soteradefense.dga.graphx.louvain.HDFSLouvainRunner
import com.soteradefense.dga.graphx.neighboringcommunity.HDFSNeighboringCommunityRunner
import com.soteradefense.dga.graphx.parser.CommandLineParser
import com.soteradefense.dga.graphx.pr.HDFSPRRunner
import com.soteradefense.dga.graphx.wcc.HDFSWCCRunner
import com.typesafe.config.ConfigFactory
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Object for kicking off analytics.
 */
object DGARunner {

  val WeaklyConnectedComponents = "wcc"
  val HighBetweennessSetExtraction = "hbse"
  val LouvainModularity = "louvain"
  val PageRank = "pr"
  val LeafCompression = "lc"
  val PageRankGraphX = "prGraphX"
  val WeaklyConnectedComponentsGraphX = "wccGraphX"
  val NeighboringCommunities = "neighboringCommunities"

  val MinProgressConfiguration = "minProgress"
  val MinProgressDefaultConfiguration = "2000"
  val ProgressCounterConfiguration = "progressCounter"
  val ProgressCounterDefaultConfiguration = "1"
  val DeltaConvergenceConfiguration = "delta"
  val DeltaConvergenceDefaultConfiguration = "0.001"
  val ParallelismConfiguration = "parallelism"
  val ParallelismDefaultConfiguration = "-1"
  val DefaultJarSplitDelimiter = ","
  val CommunitySplitConfiguration = "community.split"
  val CommunitySplitDefaultDelimiter = ":"

  /**
   * Main method for parsing arguments and running analytics.
   * @param args Commandline arguments.
   */
  def main(args: Array[String]) {
    val analytic = args(0)
    println(s"Analytic: $analytic")
    val newArgs = args.slice(1, args.length)
    val applicationConfig = ConfigFactory.load()
    val commandLineConfig: Config = CommandLineParser.parseCommandLine(newArgs)
    commandLineConfig.systemProperties.foreach({
      case (systemPropertyKey, systemPropertyValue) =>
        System.setProperty(systemPropertyKey, systemPropertyValue)
    })
    val sparkConf = buildSparkConf(commandLineConfig, applicationConfig)
    val sparkContext = new SparkContext(sparkConf)
    val parallelism = Integer.parseInt(commandLineConfig.customArguments.getOrElse(ParallelismConfiguration, applicationConfig.getString("parallelism")))
    var inputFormat: EdgeInputFormat = null
    val hdfsUrl = applicationConfig.getString("hdfs.url")
    val inputPath = hdfsUrl + commandLineConfig.inputPath
    var outputPath = hdfsUrl + commandLineConfig.outputPath
    outputPath = if (outputPath.endsWith("/")) outputPath else outputPath + "/"
    outputPath = outputPath + (new Date).getTime
    if (parallelism != -1)
      inputFormat = new EdgeInputFormat(inputPath, commandLineConfig.edgeDelimiter, parallelism)
    else
      inputFormat = new EdgeInputFormat(inputPath, commandLineConfig.edgeDelimiter)
//    val edgeRDD = inputFormat.getEdgeRDD(sparkContext)
//    val initialGraph = Graph.fromEdges(edgeRDD, None)
    val initialGraph = inputFormat.getGraphFromStringEdgeList(sparkContext)
    var runner: Harness = null

    analytic match {
      case WeaklyConnectedComponents | WeaklyConnectedComponentsGraphX =>
        runner = new HDFSWCCRunner(outputPath, commandLineConfig.edgeDelimiter)
      case HighBetweennessSetExtraction =>
        runner = new HDFSHBSERunner(outputPath, commandLineConfig.edgeDelimiter)
      case LouvainModularity =>
        val minProgress = commandLineConfig.customArguments.getOrElse(MinProgressConfiguration, MinProgressDefaultConfiguration).toInt
        val progressCounter = commandLineConfig.customArguments.getOrElse(ProgressCounterConfiguration, ProgressCounterDefaultConfiguration).toInt
        runner = new HDFSLouvainRunner(minProgress, progressCounter, outputPath)
      case LeafCompression =>
        runner = new HDFSLCRunner(outputPath, commandLineConfig.edgeDelimiter)
      case PageRank | PageRankGraphX =>
        val delta = commandLineConfig.customArguments.getOrElse(DeltaConvergenceConfiguration, DeltaConvergenceDefaultConfiguration).toDouble
        runner = new HDFSPRRunner(outputPath, commandLineConfig.edgeDelimiter, delta)
      case NeighboringCommunities =>
        val minProgress = commandLineConfig.customArguments.getOrElse(MinProgressConfiguration, MinProgressDefaultConfiguration).toInt
        val progressCounter = commandLineConfig.customArguments.getOrElse(ProgressCounterConfiguration, ProgressCounterDefaultConfiguration).toInt
        val communitySplit = commandLineConfig.customArguments.getOrElse(CommunitySplitConfiguration, CommunitySplitDefaultDelimiter)
        runner = new HDFSNeighboringCommunityRunner(minProgress, progressCounter, outputPath, commandLineConfig.edgeDelimiter, communitySplit)
      case _ =>
        throw new IllegalArgumentException(s"$analytic is not supported")
    }
    analytic match {
      case WeaklyConnectedComponents |
           HighBetweennessSetExtraction |
           LouvainModularity |
           LeafCompression |
           PageRank |
           NeighboringCommunities =>
        runner.run(sparkContext, initialGraph)
      case PageRankGraphX =>
        runner.asInstanceOf[HDFSPRRunner].runGraphXImplementation(initialGraph)
      case WeaklyConnectedComponentsGraphX =>
        runner.asInstanceOf[HDFSWCCRunner].runGraphXImplementation(initialGraph)
    }
  }

  private def buildSparkConf(commandLineConfig: Config, applicationConfig: com.typesafe.config.Config): SparkConf = {
    val sparkConf = new SparkConf()
      .setAppName(commandLineConfig.sparkAppName)
      .setJars(commandLineConfig.sparkJars.split(DefaultJarSplitDelimiter))
    if (applicationConfig.hasPath("spark.master.url"))
      sparkConf.setMaster(applicationConfig.getString("spark.master.url"))
    if (applicationConfig.hasPath("spark.home"))
      sparkConf.setSparkHome(applicationConfig.getString("spark.home"))
    sparkConf.setAll(commandLineConfig.customArguments)
    if (commandLineConfig.useKryoSerializer)
      GraphXUtils.registerKryoClasses(sparkConf)

    sparkConf
  }
}
