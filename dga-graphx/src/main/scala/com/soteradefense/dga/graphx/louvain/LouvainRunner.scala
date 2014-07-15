package com.soteradefense.dga.graphx.louvain

import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import com.soteradefense.dga.graphx.parser.CommandLineParser
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Execute the louvain distributed community detection.
 * Requires an edge file and output directory in hdfs (local files for local mode only)
 */
object LouvainRunner {

  def main(args: Array[String]) {

    val cmdLine = new CommandLineParser().parseCommandLine(args)

    // set system properties
    cmdLine.properties.foreach({ case (k, v) =>
      println(s"System.setProperty($k, $v)")
      System.setProperty(k, v)
    })

    // Create the spark context
    val conf = new SparkConf().setMaster(cmdLine.master)
      .setAppName(cmdLine.appName)
      .setSparkHome(cmdLine.sparkHome)
      .setJars(cmdLine.jars.split(","))
    val sc = new SparkContext(conf)
    // read the input into a distributed edge list
    val inputHashFunc = if (cmdLine.customArguments.getOrElse("ipAddress", "false").toBoolean) (id: String) => IpAddress.toLong(id) else (id: String) => id.toLong
    val inputFormat = new EdgeInputFormat(cmdLine.input, cmdLine.edgeDelimiter)
    var edgeRDD = inputFormat.getEdgeRDD(sc, inputHashFunc)

    // if the parallelism option was set map the input to the correct number of partitions,
    // otherwise parallelism will be based off number of HDFS blocks
    val parallelism = Integer.parseInt(cmdLine.customArguments.getOrElse("parallelism", "-1"))
    if (parallelism != -1) edgeRDD = edgeRDD.coalesce(parallelism, shuffle = true)

    // create the graph
    val graph = Graph.fromEdges(edgeRDD, None)
    val minProgress = Integer.parseInt(cmdLine.customArguments.getOrElse("minProgress", "2000"))
    val progressCounter = Integer.parseInt(cmdLine.customArguments.getOrElse("progressCounter", "1"))
    // use a helper class to execute the louvain
    // algorithm and save the output.
    // to change the outputs you can extend LouvainRunner.scala
    val runner = new HDFSLouvainRunner(minProgress, progressCounter, cmdLine.output)
    runner.run(sc, graph)


  }


}



