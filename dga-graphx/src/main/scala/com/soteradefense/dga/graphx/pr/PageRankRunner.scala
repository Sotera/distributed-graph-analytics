package com.soteradefense.dga.graphx.pr

import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import com.soteradefense.dga.graphx.parser.CommandLineParser
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}


object PageRankRunner {
  def main(args: Array[String]) {
    val cmdLine = new CommandLineParser().parseCommandLine(args)
    cmdLine.properties.foreach({ case (k, v) => System.setProperty(k, v)})
    val conf = new SparkConf().setMaster(cmdLine.master)
      .setAppName(cmdLine.appName)
      .setSparkHome(cmdLine.sparkHome)
      .setJars(cmdLine.jars.split(","))
    val sc = new SparkContext(conf)
    val inputFormat = new EdgeInputFormat(cmdLine.input, cmdLine.edgeDelimiter)
    val edgeRDD = inputFormat.getEdgeRDD(sc)
    val graph = Graph.fromEdges(edgeRDD, None)
    val delta = cmdLine.customArguments.getOrElse("delta", "0.001").toDouble
    val runner = new HDFSPRRunner(cmdLine.output, cmdLine.edgeDelimiter, delta)
    runner.run(sc, graph)
  }
}