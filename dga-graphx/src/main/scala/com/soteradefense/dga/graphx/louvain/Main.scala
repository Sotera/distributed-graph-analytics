package com.soteradefense.dga.graphx.louvain

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._



// specify command line optoins
case class Config(
    input:String = "",
    output: String = "",
    master:String="local",
    appName:String="graphX analytic",
    jars:String="",
    sparkHome:String="",
    properties:Seq[(String,String)]= Seq.empty[(String,String)] )

    
object Main {
  
  def main(args: Array[String]) {
    
    // Parse Command line options
    val parser = new scopt.OptionParser[Config](this.getClass().toString()){
      opt[String]('i',"input") action {(x,c)=> c.copy(input=x)}  text("input file or path")
      opt[String]('o',"output") action {(x,c)=> c.copy(output=x)} text("output path")
      opt[String]('m',"master") action {(x,c)=> c.copy(master=x)} text("spark master, local[N] or spark://host:port")
      opt[String]('h',"sparkhome") action {(x,c)=> c.copy(sparkHome=x)} text("SPARK_HOME")
      opt[String]('n',"jobname") action {(x,c)=> c.copy(appName=x)} text("job name")
      opt[String]('j',"jars") action {(x,c)=> c.copy(jars=x)} text("comma seperated list of jars")
      arg[(String,String)]("<property>=<value>....") unbounded() optional() action {case((k,v),c)=> c.copy(properties = c.properties :+ (k,v)) }
    }
    var edgeFile, outputdir,master,jobname,jars,sparkhome = ""
    var properties:Seq[(String,String)]= Seq.empty[(String,String)]
    parser.parse(args,Config()) map {
      config =>
        edgeFile = config.input
        outputdir = config.output
        master = config.master
        jobname = config.appName
        jars = config.jars
        sparkhome = config.sparkHome
        properties = config.properties
        if (edgeFile == "" || outputdir == "") {
          println(parser.usage)
          sys.exit(1)
        }
    } getOrElse{
      sys.exit(1)
    }
    
    // set system properties
   var minSplits = 8
    properties.foreach( {case (k,v)=>
      if (k == "spark.default.parallelism") minSplits = v.toInt
      println(s"System.setProperty($k, $v)")
      System.setProperty(k, v)
    })
    
    // Create the spark context
    var sc: SparkContext = null
    if (master.indexOf("local") == 0 ){
      println(s"sparkcontext = new SparkContext($master,$jobname)")
      sc = new SparkContext(master, jobname)
    }
    else{
      println(s"sparkcontext = new SparkContext($master,$jobname,$sparkhome,$jars)")
      sc = new SparkContext(master,jobname,sparkhome,jars.split(","))
    }
   
    
    // read the input into a distributed edge list
    val edgeRDD = sc.textFile(edgeFile,minSplits).map(row=> {
      val tokens = row.split("\t").map(_.trim().toLong)
      tokens.length match {
        case 2 => {new Edge(tokens(0),tokens(1),1L) }
        case 3 => {new Edge(tokens(0),tokens(1),tokens(2))}
        case _ => {throw new IllegalArgumentException("invalid input line: "+row)}
      }
    })  
    
    // create the graph
    val graph = Graph.fromEdges(edgeRDD, None)
    
    // use a helper class to execute the louvain
    // algorithm and save the output.
    // to change the outputs you can extend LouvainRunner.scala
    val runner = new HDFSLouvainRunner(outputdir)
    runner.run(sc, graph)
    

  }
  

  
}



