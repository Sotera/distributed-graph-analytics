package com.soteradefense.dga.graphx.config

import scala.collection.mutable

case class Config(
                   input: String = "",
                   output: String = "",
                   master: String = "local",
                   appName: String = "GraphX Analytic",
                   jars: String = "",
                   sparkHome: String = "",
                   parallelism: Int = -1,
                   edgeDelimiter: String = ",",
                   kryo: Boolean = false,
                   properties: Seq[(String, String)] = Seq.empty[(String, String)],
                   customArguments: mutable.HashMap[String, String] = mutable.HashMap.empty[String, String])
