package com.soteradefense.dga.graphx.wcc

import com.esotericsoftware.minlog.Log
import com.soteradefense.dga.graphx.config.Config
import org.apache.spark.graphx.{Edge, Graph, GraphKryoRegistrator}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Config](this.getClass.toString) {

      opt[String]('i', "inputPath") action { (x, c) => c.copy(input = x)} text "Input path in HDFS"
      opt[String]('o', "outputPath") action { (x, c) => c.copy(output = x)} text "Output path in HDFS"
      opt[String]('d', "delimiter") action { (x, c: Config) => c.copy(edgeDelimiter = x)} text "Input Delimiter"
      opt[String]('m', "master") action { (x, c) => c.copy(master = x)} text "Spark Master, local[N] or spark://host:port default=local"
      opt[String]('h', "sparkHome") action { (x, c) => c.copy(sparkHome = x)} text "SPARK_HOME Required to run on a cluster"
      opt[String]('n', "jobName") action { (x, c) => c.copy(appName = x)} text "Job Name"
      opt[String]('j', "jars") action { (x, c) => c.copy(jars = x)} text "Comma Separated List of jars"
      arg[(String, String)]("<property>=<value>....") unbounded() optional() action { case ((k, v), c) => c.copy(properties = c.properties :+(k, v))}
    }
    var cmdLine = Config()
    parser.parse(args, Config()) map {
      config =>
        cmdLine = config
    } getOrElse sys.exit(1)
    cmdLine.properties.foreach({ case (k, v) => System.setProperty(k, v)})
    val conf = new SparkConf().setMaster(cmdLine.master)
      .setAppName(cmdLine.appName)
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      //.set("spark.kryo.registrator", classOf[DGARegistrator].getCanonicalName)
      .set("spark.kryo.registrator", classOf[GraphKryoRegistrator].getCanonicalName)
      .setSparkHome(cmdLine.sparkHome)
      .setJars(cmdLine.jars.split(","))
    val sc = new SparkContext(conf)
    Log.TRACE()
    //    val textData = sc.textFile(cmdLine.input)
    //    val inputFormat = new EdgeInputFormat(textData, cmdLine.edgeDelimiter)
    //    val edgeRDD = inputFormat.getEdgeRDD()
    val typeConversionMethod: String => Long = _.toLong
    val edgeRDD = sc.textFile(cmdLine.input).map(row => {
      val tokens = row.split(cmdLine.edgeDelimiter).map(_.trim())
      tokens.length match {
        case 2 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), 1L)
        case 3 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), tokens(2).toLong)
        case _ => throw new IllegalArgumentException("invalid input line: " + row)
      }
    })
    val graph = Graph.fromEdges(edgeRDD, None)
    graph.connectedComponents().triplets.map(t => s"${t.srcId}${cmdLine.edgeDelimiter}${t.dstId}${cmdLine.edgeDelimiter}${t.srcAttr}").saveAsTextFile(cmdLine.output)
  }
}