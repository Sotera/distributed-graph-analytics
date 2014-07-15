package com.soteradefense.dga.graphx

import com.soteradefense.dga.graphx.harness.Harness
import com.soteradefense.dga.graphx.hbse.HDFSHBSERunner
import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import com.soteradefense.dga.graphx.lc.HDFSLCRunner
import com.soteradefense.dga.graphx.louvain.HDFSLouvainRunner
import com.soteradefense.dga.graphx.parser.CommandLineParser
import com.soteradefense.dga.graphx.pr.HDFSPRRunner
import com.soteradefense.dga.graphx.wcc.HDFSWCCRunner
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}

object DGARunner {
  def main(args: Array[String]) {
    val analytic = args(0)
    println(s"Analytic: $analytic")
    val newArgs = args.slice(1, args.length)
    //PropertyConfigurator.configure(getClass.getResource("log4j.properties"))
    val cmdLine = new CommandLineParser().parseCommandLine(newArgs)
    cmdLine.properties.foreach({ case (k, v) => System.setProperty(k, v)})
    val conf = new SparkConf().setMaster(cmdLine.master)
      .setAppName(cmdLine.appName)
      .setSparkHome(cmdLine.sparkHome)
      .setJars(cmdLine.jars.split(","))
    conf.setAll(cmdLine.customArguments)
    //.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    //.set("spark.kryo.registrator", classOf[DGAKryoRegistrator].getCanonicalName)
    val sc = new SparkContext(conf)
    val inputFormat = new EdgeInputFormat(cmdLine.input, cmdLine.edgeDelimiter)
    var edgeRDD = inputFormat.getEdgeRDD(sc)
    val parallelism = Integer.parseInt(cmdLine.customArguments.getOrElse("parallelism", "-1"))
    if (parallelism != -1)
      edgeRDD = edgeRDD.coalesce(parallelism, shuffle = true)
    val graph = Graph.fromEdges(edgeRDD, None)
    var runner: Harness = null
    if (analytic.equals("wcc")) {
      runner = new HDFSWCCRunner(cmdLine.output, cmdLine.edgeDelimiter)
    }
    else if (analytic.equals("hbse")) {
      runner = new HDFSHBSERunner(cmdLine.output, cmdLine.edgeDelimiter)
    }
    else if (analytic.equals("louvain")) {
      val minProgress = Integer.parseInt(cmdLine.customArguments.getOrElse("minProgress", "2000"))
      val progressCounter = Integer.parseInt(cmdLine.customArguments.getOrElse("progressCounter", "1"))
      runner = new HDFSLouvainRunner(minProgress, progressCounter, cmdLine.output)
    }
    else if (analytic.equals("lc")) {
      runner = new HDFSLCRunner(cmdLine.output, cmdLine.edgeDelimiter)
    }
    else if (analytic.equals("pr")) {
      val delta = cmdLine.customArguments.getOrElse("delta", "0.001").toDouble
      runner = new HDFSPRRunner(cmdLine.output, cmdLine.edgeDelimiter, delta)
    }
    else {
      throw new IllegalArgumentException(s"$analytic is not supported")
    }
    runner.run(sc, graph)
  }
}
