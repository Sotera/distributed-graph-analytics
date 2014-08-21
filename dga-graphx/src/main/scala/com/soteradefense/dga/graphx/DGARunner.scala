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

import com.soteradefense.dga.graphx.config.Config
import com.soteradefense.dga.graphx.harness.Harness
import com.soteradefense.dga.graphx.hbse.HDFSHBSERunner
import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import com.soteradefense.dga.graphx.kryo.DGAKryoRegistrator
import com.soteradefense.dga.graphx.lc.HDFSLCRunner
import com.soteradefense.dga.graphx.louvain.HDFSLouvainRunner
import com.soteradefense.dga.graphx.parser.CommandLineParser
import com.soteradefense.dga.graphx.pr.HDFSPRRunner
import com.soteradefense.dga.graphx.wcc.HDFSWCCRunner
import org.apache.spark.graphx.Graph
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

  val MinProgressConfiguration = "minProgress"
  val MinProgressDefaultConfiguration = "2000"
  val ProgressCounterConfiguration = "progressCounter"
  val ProgressCounterDefaultConfiguration = "1"
  val DeltaConvergenceConfiguration = "delta"
  val DeltaConvergenceDefaultConfiguration = "0.001"
  val ParallelismConfiguration = "parallelism"
  val ParallelismDefaultConfiguration = "-1"
  val DefaultJarSplitDelimiter = ","


  /**
   * Main method for parsing arguments and running analytics.
   * @param args Commandline arguments.
   */
  def main(args: Array[String]) {
    val analytic = args(0)
    println(s"Analytic: $analytic")
    val newArgs = args.slice(1, args.length)
    val commandLineConfig: Config = CommandLineParser.parseCommandLine(newArgs)
    commandLineConfig.systemProperties.foreach({
      case (systemPropertyKey, systemPropertyValue) =>
        System.setProperty(systemPropertyKey, systemPropertyValue)
    })
    val conf = new SparkConf()
      .setMaster(commandLineConfig.sparkMasterUrl)
      .setAppName(commandLineConfig.sparkAppName)
      .setSparkHome(commandLineConfig.sparkHome)
      .setJars(commandLineConfig.sparkJars.split(DefaultJarSplitDelimiter))
    conf.setAll(commandLineConfig.customArguments)
    if (commandLineConfig.useKryoSerializer) {
      conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      conf.set("spark.kryo.registrator", classOf[DGAKryoRegistrator].getCanonicalName)
    }
    val sparkContext = new SparkContext(conf)
    val parallelism = Integer.parseInt(commandLineConfig.customArguments.getOrElse(ParallelismConfiguration, ParallelismDefaultConfiguration))
    var inputFormat: EdgeInputFormat = null
    if (parallelism != -1)
      inputFormat = new EdgeInputFormat(commandLineConfig.inputPath, commandLineConfig.edgeDelimiter, parallelism)
    else
      inputFormat = new EdgeInputFormat(commandLineConfig.inputPath, commandLineConfig.edgeDelimiter)
    val edgeRDD = inputFormat.getEdgeRDD(sparkContext)
    val initialGraph = Graph.fromEdges(edgeRDD, None)
    var runner: Harness = null
    analytic match {
      case WeaklyConnectedComponents | WeaklyConnectedComponentsGraphX =>
        runner = new HDFSWCCRunner(commandLineConfig.outputPath, commandLineConfig.edgeDelimiter)
      case HighBetweennessSetExtraction =>
        runner = new HDFSHBSERunner(commandLineConfig.outputPath, commandLineConfig.edgeDelimiter)
      case LouvainModularity =>
        val minProgress = commandLineConfig.customArguments.getOrElse(MinProgressConfiguration, MinProgressDefaultConfiguration).toInt
        val progressCounter = commandLineConfig.customArguments.getOrElse(ProgressCounterConfiguration, ProgressCounterDefaultConfiguration).toInt
        runner = new HDFSLouvainRunner(minProgress, progressCounter, commandLineConfig.outputPath)
      case LeafCompression =>
        runner = new HDFSLCRunner(commandLineConfig.outputPath, commandLineConfig.edgeDelimiter)
      case PageRank | PageRankGraphX =>
        val delta = commandLineConfig.customArguments.getOrElse(DeltaConvergenceConfiguration, DeltaConvergenceDefaultConfiguration).toDouble
        runner = new HDFSPRRunner(commandLineConfig.outputPath, commandLineConfig.edgeDelimiter, delta)
      case _  =>
        throw new IllegalArgumentException(s"$analytic is not supported")
    }
    analytic match {
      case WeaklyConnectedComponents | HighBetweennessSetExtraction | LouvainModularity | LeafCompression | PageRank =>
        runner.run(sparkContext, initialGraph)
      case PageRankGraphX =>
        runner.asInstanceOf[HDFSPRRunner].runGraphXImplementation(initialGraph)
      case WeaklyConnectedComponentsGraphX =>
        runner.asInstanceOf[HDFSWCCRunner].runGraphXImplementation(initialGraph)
    }
  }
}
