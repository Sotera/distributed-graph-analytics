package com.soteradefense.dga.graphx.parser

import com.soteradefense.dga.graphx.config.Config

class CommandLineParser {

  def parseCommandLine(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config](this.getClass.toString) {
      head("dga-giraph", "0.1")
      opt[String]('i', "inputPath") action { (x, c) => c.copy(input = x)} text "Input path in HDFS"
      opt[String]('o', "outputPath") action { (x, c) => c.copy(output = x)} text "Output path in HDFS"
      opt[String]('d', "delimiter") action { (x, c: Config) => c.copy(edgeDelimiter = x)} text "Input Delimiter"
      opt[String]('m', "master") action { (x, c) => c.copy(master = x)} text "Spark Master, local[N] or spark://host:port default=local"
      opt[String]('s', "sparkHome") action { (x, c) => c.copy(sparkHome = x)} text "SPARK_HOME Required to run on a cluster"
      opt[String]('n', "jobName") action { (x, c) => c.copy(appName = x)} text "Job Name"
      opt[String]('j', "jars") action { (x, c) => c.copy(jars = x)} text "Comma Separated List of jars"
      help("help") text "prints this usage text"
      opt[(String, String)]("ca") unbounded() optional() action { case ((k, v), c) => c.copy(customArguments = c.customArguments += k -> v)} keyValueName("<argumentstring>", "<argumentvalue>")
      arg[(String, String)]("<property>=<value>....") unbounded() optional() action { case ((k, v), c) => c.copy(properties = c.properties :+(k, v))}
    }
    var cmdLine = Config()
    parser.parse(args, Config()) map {
      config =>
        cmdLine = config
    } getOrElse sys.exit(1)
    cmdLine
  }
}
