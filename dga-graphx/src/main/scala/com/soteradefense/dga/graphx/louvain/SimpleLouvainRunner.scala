package com.soteradefense.dga.graphx.louvain

import java.util.Date


import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}


object SimpleLouvainRunner {


  /**
   * Main method for parsing arguments and running analytics.
   * @param args Commandline arguments.
   */
  def main(args: Array[String]) {
    println("Welcome to the simple yarn runner")

    if (args.length < 2 || args(0) == "-h" || args(0) == "--help") {
      println("arguments: inputPath outputPath [minProgress] [progressCounter]")
      sys.exit(1)
    }

    val inputPath = args(0)
    var outputPath = args(1)
    val minProgress = if (args.length > 2) args(2).toInt else 2000
    val progressCounter = if (args.length > 3) args(3).toInt else 1

    val sparkConf = new SparkConf()
    sparkConf.getAll.foreach(println(_))
    val sc = new SparkContext(sparkConf)


    val inputFormat = new EdgeInputFormat(inputPath, ",")
    val edgeRDD = inputFormat.getEdgeRDD(sc)
    val initialGraph = Graph.fromEdges(edgeRDD, None)


    val runner = new HDFSLouvainRunner(minProgress, progressCounter, outputPath)
    runner.run(sc, initialGraph)


  }

}