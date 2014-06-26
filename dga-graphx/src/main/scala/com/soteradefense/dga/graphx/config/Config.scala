package com.soteradefense.dga.graphx.config

case class Config(
                   input: String = "",
                   output: String = "",
                   master: String = "local",
                   appName: String = "graphX analytic",
                   jars: String = "",
                   sparkHome: String = "",
                   parallelism: Int = -1,
                   edgeDelimiter: String = ",",
                   ipAddress: Boolean = false,
                   properties: Seq[(String, String)] = Seq.empty[(String, String)])