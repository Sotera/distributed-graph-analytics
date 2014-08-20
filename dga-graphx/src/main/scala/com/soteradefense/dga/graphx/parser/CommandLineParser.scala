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
package com.soteradefense.dga.graphx.parser

import com.soteradefense.dga.graphx.config.Config

/**
 * Class that parses the command line arguments for the runner.
 */
class CommandLineParser {

  /**
   * Method that parses the command line arguments.
   * @param args Array of arguments from the command line.
   * @return A Config object containing all the parsed arguments.
   */
  def parseCommandLine(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config](this.getClass.toString) {
      head("dga-graphx", "0.1")
      opt[String]('i', "inputPath") required() action { (x, c) => c.copy(input = x)} text "Input path in HDFS"
      opt[String]('o', "outputPath") required() action { (x, c) => c.copy(output = x)} text "Output path in HDFS"
      opt[String]('d', "delimiter") action { (x, c: Config) => c.copy(edgeDelimiter = x)} text "Input Delimiter"
      opt[String]('m', "master") action { (x, c) => c.copy(master = x)} text "Spark Master, local[N] or spark://host:port default=local"
      opt[String]('s', "sparkHome") action { (x, c) => c.copy(sparkHome = x)} text "SPARK_HOME Required to run on a cluster"
      opt[String]('n', "jobName") action { (x, c) => c.copy(appName = x)} text "Job Name"
      opt[String]('j', "jars") action { (x, c) => c.copy(jars = x)} text "Comma Separated List of jars"
      opt[Boolean]('k', "kryo") action { (x, c) => c.copy(kryo = x)} text "Use the Kryo Serializer"
      help("help") text "prints this usage text"
      opt[(String, String)]("ca") unbounded() optional() action { case ((k, v), c) => c.copy(customArguments = c.customArguments += k -> v)} keyValueName("<argumentstring>",
        "<argumentvalue>") text "Custom Properties that apply to the job."
      opt[(String, String)]("S") unbounded() optional() action { case ((k, v), c) => c.copy(properties = c.properties :+(k, v))} keyValueName("<argumentstring>",
        "<argumentvalue>") text "System Properties"
    }
    var cmdLine = Config()
    parser.parse(args, Config()) map {
      config =>
        cmdLine = config
    } getOrElse {
      throw new IllegalArgumentException("You need to specify the required arguments!")
    }
    cmdLine
  }
}
