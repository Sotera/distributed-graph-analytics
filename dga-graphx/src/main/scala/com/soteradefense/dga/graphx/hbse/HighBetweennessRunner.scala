package com.soteradefense.dga.graphx.hbse

import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import com.soteradefense.dga.graphx.parser.CommandLineParser
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.PropertyConfigurator


object HighBetweennessRunner {
  def main(args: Array[String]) {
    val cmdLine = new CommandLineParser().parseCommandLine(args)
    PropertyConfigurator.configure(getClass.getResource("/log4j.properties"))
    cmdLine.properties.foreach({ case (k, v) => System.setProperty(k, v)})
    val conf = new SparkConf().setMaster(cmdLine.master)
      .setAppName(cmdLine.appName)
      .setSparkHome(cmdLine.sparkHome)
      .setJars(cmdLine.jars.split(","))
    conf.setAll(cmdLine.customArguments)
    val sc = new SparkContext(conf)
    val inputFormat = new EdgeInputFormat(cmdLine.input, cmdLine.edgeDelimiter)
    val edgeRDD = inputFormat.getEdgeRDD(sc)
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new HDFSHBSERunner(cmdLine.output, cmdLine.edgeDelimiter)
    runner.run(sc, graph)
  }
}
