package com.soteradefense.dga.graphx.wcc

import com.soteradefense.dga.graphx.io.formats.EdgeInputFormat
import com.soteradefense.dga.graphx.parser.CommandLineParser
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val cmdLine = new CommandLineParser().parseCommandLine(args)
    cmdLine.properties.foreach({ case (k, v) => System.setProperty(k, v)})
    val conf = new SparkConf().setMaster(cmdLine.master)
      .setAppName(cmdLine.appName)
      //.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      //.set("spark.kryo.registrator", classOf[DGAKryoRegistrator].getCanonicalName)
      //.set("spark.kryo.registrator", classOf[GraphKryoRegistrator].getCanonicalName)
      .setSparkHome(cmdLine.sparkHome)
      .setJars(cmdLine.jars.split(","))
    val sc = new SparkContext(conf)
    val inputFormat = new EdgeInputFormat(cmdLine.input, cmdLine.edgeDelimiter)
    val edgeRDD = inputFormat.getEdgeRDD(sc)
    val graph = Graph.fromEdges(edgeRDD, None)
    val runner = new HDFSWCCRunner(cmdLine.output, cmdLine.edgeDelimiter)
    runner.run(graph)


    // This portion works with kryo
    //    val typeConversionMethod: String => Long = _.toLong
    //    val edgeRDD = sc.textFile(cmdLine.input).map(row => {
    //      val tokens = row.split(cmdLine.edgeDelimiter).map(_.trim())
    //      tokens.length match {
    //        case 2 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), 1L)
    //        case 3 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), tokens(2).toLong)
    //        case _ => throw new IllegalArgumentException("invalid input line: " + row)
    //      }
    //    })
    //    val graph = Graph.fromEdges(edgeRDD, None)
    //    graph.connectedComponents().triplets.map(t => s"${t.srcId}${cmdLine.edgeDelimiter}${t.dstId}${cmdLine.edgeDelimiter}${t.srcAttr}").saveAsTextFile(cmdLine.output)
  }
}