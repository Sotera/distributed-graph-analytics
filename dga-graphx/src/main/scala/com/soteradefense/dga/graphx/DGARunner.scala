package com.soteradefense.dga.graphx

import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import com.soteradefense.dga.graphx.parser.CommandLineParser
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}

//TODO: Inject a string to long method or process.
object DGARunner {
  def main(args: Array[String]) {
    val analytic = args(0)
    val newArgs = new Array[String](args.length - 1)
    Array.copy(args, 1, newArgs, 0, newArgs.length)
    val cmdLine = new CommandLineParser().parseCommandLine(newArgs)
    cmdLine.properties.foreach({ case (k, v) => System.setProperty(k, v)})
    val conf = new SparkConf().setMaster(cmdLine.master)
      .setAppName(cmdLine.appName)
      .setSparkHome(cmdLine.sparkHome)
      .setJars(cmdLine.jars.split(","))
    val sc = new SparkContext(conf)
    val inputFormat = new EdgeInputFormat(cmdLine.input, cmdLine.edgeDelimiter)
    val edgeRDD = inputFormat.getEdgeRDD(sc)
    val graph = Graph.fromEdges(edgeRDD, None)

    if (analytic.equals("wcc")) {
      wcc.Main.main(newArgs)
    }
    else if (analytic.equals("hbse")) {
      hbse.Main.main(newArgs)
    }
    else if (analytic.equals("louvain")) {
      louvain.Main.main(newArgs)
    }
    else if (analytic.equals("lc")) {
      lc.Main.main(newArgs)
    }
    else if (analytic.equals("pr")) {
      pr.Main.main(newArgs)
    }
    else {
      sys.exit(1)
    }
  }
}
