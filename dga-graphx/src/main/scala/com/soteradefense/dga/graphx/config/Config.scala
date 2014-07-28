package com.soteradefense.dga.graphx.config

import scala.collection.mutable

case class Config(
                   input: String = "",
                   output: String = "",
                   master: String = "local",
                   appName: String = "GraphX Analytic",
                   jars: String = "lib/dga-graphx-0.1.jar,lib/dga-core-0.0.1.jar,lib/spark-graphx_2.10-1.0.1.jar",
                   sparkHome: String = "",
                   parallelism: Int = -1,
                   edgeDelimiter: String = ",",
                   kryo: Boolean = false,
                   properties: Seq[(String, String)] = Seq.empty[(String, String)],
                   customArguments: mutable.HashMap[String, String] = mutable.HashMap.empty[String, String])
