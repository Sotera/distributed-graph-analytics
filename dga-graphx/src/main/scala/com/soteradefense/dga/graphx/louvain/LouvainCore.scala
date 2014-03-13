package com.soteradefense.dga.graphx.louvain

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.Graph.graphToGraphOps
import scala.math.BigDecimal.double2bigDecimal




object LouvainAlgorithm {

  /**
   * Transform a graph from [VD,Long] to a a [VertexState,Long] graph and compute louvain on it.
   */
  def louvainFromStandardGraph[VD: ClassTag](sc:SparkContext,graph:Graph[VD,Long]) : (Double,Graph[VertexState,Long]) = {
	  val louvainGraph = createLouvainGraph(graph)
	  return louvain(sc,louvainGraph)
  }
  
  
  /**
   * compute louvain communities on a graph of type [VertexState,Long]
   */
  def louvain(sc:SparkContext, graph:Graph[VertexState,Long]) : (Double,Graph[VertexState,Long])= {
    var louvainGraph = graph.cache()
    val minProgress = 2000
    val progressCounter = 1
    val edgeWeights =2* louvainGraph.edges.map(_.attr).reduce(_+_)
    val internalWeights = louvainGraph.vertices.values.map(vdata=>vdata.internalWeight).reduce(_+_)
    var totalEdgeWeight = sc.broadcast(edgeWeights+internalWeights) 
    println("totalEdgeWeight: "+totalEdgeWeight.value)
    var msgRDD = louvainGraph.mapReduceTriplets(sendMsg,mergeMsg)
    var activeMessages = msgRDD.count()
     
    var updated = 0L - minProgress
    var even = false
    var count = 0
    val maxIter = 100 
    var stop = 0
    var updatedLastPhase = 0L
    do { 
       count += 1
	   even = ! even
	   
	   println("\nCYCLE "+count)
	   //printlouvain(louvainGraph)
       //printedgetriplets(louvainGraph)
	   
	  
	   // label each vertex with its best community based on neighboring community information
	   val labeledVerts = louvainVertJoin(louvainGraph,msgRDD,totalEdgeWeight,even).cache()   
	   
	   // calculate new sigma total value for each community (total weight of each community)
	   val communtiyUpdate = labeledVerts
	    .map( {case (vid,vdata) => (vdata.community,vdata.nodeWeight+vdata.internalWeight)})
	   .reduceByKey(_+_).cache()
	   
	   // map each vertex ID to its updated community information
	   val communityMapping = labeledVerts
	     .map( {case (vid,vdata) => (vdata.community,vid)})
	     .join(communtiyUpdate)
	     .map({case (community,(vid,sigmaTot)) => (vid,(community,sigmaTot)) })
	   .cache()
	   
	   // join the community labeled vertices with the updated community info
	   val updatedVerts = labeledVerts.join(communityMapping).map({ case (vid,(vdata,communityTuple) ) => 
	     vdata.community = communityTuple._1  
	     vdata.communitySigmaTot = communityTuple._2
	     (vid,vdata)
	   }).cache()
	   updatedVerts.count()
	   labeledVerts.unpersist(blocking = false)
	   communtiyUpdate.unpersist(blocking=false)
	   communityMapping.unpersist(blocking=false)
	   
	   val prevG = louvainGraph
	   louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
	   louvainGraph.cache()
	   
       val oldMsgs = msgRDD
       msgRDD = louvainGraph.mapReduceTriplets(sendMsg, mergeMsg).cache()
       activeMessages = msgRDD.count()  // materializes the graph by forcing computation
	 
       oldMsgs.unpersist(blocking=false)
       updatedVerts.unpersist(blocking=false)
       prevG.unpersistVertices(blocking=false)
       
	   if (even) updated = 0
	   updated = updated + louvainGraph.vertices.filter(_._2.changed).count 
	   if (!even) {
	     println("VERTICES MOVED THIS PHASE: "+updated)
	     val delta = updatedLastPhase - updated
	     if (delta > 0 && delta < minProgress) stop +=1
	     updatedLastPhase = updated
	   }

   
    } while ( stop <= progressCounter && (even ||   (updated > 0 && count < maxIter)))
   
   println("\nCompleted in "+count+" cycles")
   
   
   // now we need to gather neighbor data one more time to calculate the actual Q of the graph.
   val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid,vdata,msgs)=> {
     // sum the nodes internal weight and all of its edges that are in the community
     val community = vdata.community
     var k_i_in = vdata.internalWeight
     var sigmaTot = vdata.communitySigmaTot.toDouble
     msgs.foreach({ case( (communityId,sigmaTotal),communityEdgeWeight ) => 
       if (vdata.community == communityId) k_i_in += communityEdgeWeight})
     val M = totalEdgeWeight.value
     val k_i = vdata.nodeWeight + vdata.internalWeight
     var q = (k_i_in.toDouble / M) -  ( ( sigmaTot *k_i) / math.pow(M, 2) )
      //println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
     q = if (q < 0) 0 else q
     q
   })  
   val actualQ = newVerts.values.reduce(_+_)
   
   return (actualQ,louvainGraph)
   
  }
  
  
  
  
  // calculate the change in modularity that would result from choosing a community
  private def q(currCommunityId:Long, testCommunityId:Long, testSigmaTot:Long, edgeWeightInCommunity:Long, nodeWeight:Long, internalWeight:Long, totalEdgeWeight:Long) : BigDecimal = {
	  	val isCurrentCommunity = (currCommunityId.equals(testCommunityId));
		val M = BigDecimal(totalEdgeWeight); 
	  	val k_i_in_L =  if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity;
		val k_i_in = BigDecimal(k_i_in_L);
		val k_i = BigDecimal(nodeWeight + internalWeight);
		var sigma_tot = BigDecimal(testSigmaTot);
		if (isCurrentCommunity) {
		    sigma_tot = sigma_tot - k_i
		}
		var deltaQ =  BigDecimal(0.0);
		if (!(isCurrentCommunity && sigma_tot.equals(deltaQ))) {
			val dividend = k_i * sigma_tot;
			deltaQ = k_i_in - (dividend / M)
			//println(s"      $deltaQ = $k_i_in - ( $k_i * $sigma_tot / $M")
			
		}
		return deltaQ;
  }
  
  


   // debug printing
   private def printlouvain(graph:Graph[VertexState,Long]) = {
     print("\ncommunity label snapshot\n(vid,community,sigmaTot)\n")
     graph.vertices.mapValues((vid,vdata)=> (vdata.community,vdata.communitySigmaTot)).collect().foreach(f=>println(" "+f))
   }
  
   
   // debug printing
   private def printedgetriplets(graph:Graph[VertexState,Long]) = {
     print("\ncommunity label snapshot FROM TRIPLETS\n(vid,community,sigmaTot)\n")
     (graph.triplets.flatMap(e=> Iterator((e.srcId,e.srcAttr.community,e.srcAttr.communitySigmaTot), (e.dstId,e.dstAttr.community,e.dstAttr.communitySigmaTot))).collect()).foreach(f=>println(" "+f))
   }
   
   
   /**
    * Convert an input graph into a Graph[VertexState,Long] for louvain computaion
    */
   def createLouvainGraph[VD: ClassTag](graph: Graph[VD,Long]) : Graph[VertexState,Long]= {
    // Create the initial Louvain graph.  
    val nodeWeightMapFunc = (e:EdgeTriplet[VD,Long]) => Iterator((e.srcId,e.attr), (e.dstId,e.attr))
    val nodeWeightReduceFunc = (e1:Long,e2:Long) => e1+e2
    val nodeWeights = graph.mapReduceTriplets(nodeWeightMapFunc,nodeWeightReduceFunc)
    
    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid,data,weightOption)=> { 
      val weight = weightOption.getOrElse(0L)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = weight
      state.internalWeight = 0L
      state.nodeWeight = weight
      state
    }).groupEdges(_+_)
    
    
    
    return louvainGraph
   }
   
   
  
  // for each ege triplet send a message to both src and dst
  // containing community information of the neighbor
  private def sendMsg(et:EdgeTriplet[VertexState,Long]) = {
    val m1 = (et.dstId,Map((et.srcAttr.community,et.srcAttr.communitySigmaTot)->et.attr))
	val m2 = (et.srcId,Map((et.dstAttr.community,et.dstAttr.communitySigmaTot)->et.attr))
	Iterator(m1, m2)    
  }
  
  // combine all messages, sum edge weights for each community
  private def mergeMsg(m1:Map[(Long,Long),Long],m2:Map[(Long,Long),Long]) ={
    val newMap = scala.collection.mutable.HashMap[(Long,Long),Long]()
    m1.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }
  
  
  /**
   * Join vertices from the graph with community information aggregated from all neighbors
   * Calculate best community
   * Create a new set of vertices with updated community information.
   */
  private def louvainVertJoin(louvainGraph:Graph[VertexState,Long], msgRDD:VertexRDD[Map[(Long,Long),Long]], totalEdgeWeight:Broadcast[Long], even:Boolean) = {
     louvainGraph.vertices.innerJoin(msgRDD)( (vid, vdata, msgs)=> {
	     //println("vertex: "+vid)
	     //msgs.foreach( { case ((community,sigmaTot),weight) => println("    community:"+community+" sigmaTot:"+sigmaTot+" weight: "+weight) } )
	     // calculate best community
	      var bestCommunity = vdata.community
		  var startingCommunityId = bestCommunity
		  var maxDeltaQ = BigDecimal(0.0);
	      var bestSigmaTot = 0L
	      msgs.foreach({ case( (communityId,sigmaTotal),communityEdgeWeight ) => 
	      	val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vdata.nodeWeight, vdata.internalWeight,totalEdgeWeight.value)
	        //println("   communtiy: "+communityId+" sigma:"+sigmaTotal+" edgeweight:"+communityEdgeWeight+"  q:"+deltaQ)
	      	
	        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))){
	          maxDeltaQ = deltaQ
	          bestCommunity = communityId
	          bestSigmaTot = sigmaTotal
	        }
	      })	      
	      // update community and change count
	      // only allow changes from low to high communties on even cyces and high to low on odd cycles
		  if ( vdata.community != bestCommunity && ( (even && vdata.community > bestCommunity)  || (!even && vdata.community < bestCommunity)  )  ){
		    //println("  "+vid+" SWITCHED from "+vdata.community+" to "+bestCommunity)
		    vdata.community = bestCommunity
		    vdata.communitySigmaTot = bestSigmaTot  
		    vdata.changed = true
		  }
		  else{
		    vdata.changed = false
		  }   
	     vdata
	   })
  }
  
  
  /**
   * Compress a graph by its communities, aggregate both internal node weights and edge
   * weights within communities.
   */
  def compressGraph(graph:Graph[VertexState,Long],debug:Boolean=true) : Graph[VertexState,Long] = {

    // edge weights internal to each community
    val internalEdgeWeights = graph.mapReduceTriplets (
        // map
        et=>{ 
          if (et.srcAttr.community == et.dstAttr.community){
            Iterator( ( et.srcAttr.community, 2*et.attr) )  // count the weight from both nodes  // count the weight from both nodes
          } 
          else Iterator.empty  
        },
        // reduce
        (w1:Long,w2:Long) => w1+w2 )
    
    // internal weights of nodes in each commutniy
    var internalWeights = graph.vertices.values.map(vdata=> (vdata.community,vdata.internalWeight)).reduceByKey(_+_)
   
    // each community with its internal weights, node weights still need filled in
    
    val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({case (vid,(weight1,weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0L)
      if (debug && vid.toLong == 6226L) println(s"$vid: created new state node weights: $weight1,$weight2")
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = weight1+weight2
      state.nodeWeight = 0L
      (vid,state)
    }).cache()
    
    
     val edges = graph.triplets.flatMap(et=> {
       val src = math.min(et.srcAttr.community,et.dstAttr.community)
       val dst = math.max(et.srcAttr.community,et.dstAttr.community)
       if (src != dst) Iterator(new Edge(src, dst, et.attr))
       else Iterator.empty
    }).cache()
    
    var compressedGraph = Graph(newVerts,edges).groupEdges(_+_)
    
    // calculate the weight of each node
    val nodeWeightMapFunc = (e:EdgeTriplet[VertexState,Long]) => Iterator((e.srcId,e.attr), (e.dstId,e.attr))
    val nodeWeightReduceFunc = (e1:Long,e2:Long) => e1+e2
    val nodeWeights = compressedGraph.mapReduceTriplets(nodeWeightMapFunc,nodeWeightReduceFunc)
    
    
    val louvainGraph = compressedGraph.joinVertices(nodeWeights)((vid,data,weight)=> { 
    //val louvainGraph = compressedGraph. outerJoinVertices(nodeWeights)((vid,data,weightOption)=> { 
      if (debug && null == data) println(s"$vid: data is null")
      //val weight = weightOption.getOrElse(0L)
      data.communitySigmaTot = weight +data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph
    
    newVerts.unpersist(blocking=false)
    edges.unpersist(blocking=false)
    return louvainGraph
    
   
    
  }
  

  
  
}

  

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState extends Serializable{

  var community = -1L
  var communitySigmaTot = 0L
  var internalWeight = 0L  // self edges
  var nodeWeight = 0L;  //out degree
  var changed = false
   
  override def toString(): String = {
    "{community:"+community+",communitySigmaTot:"+communitySigmaTot+
    ",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+"}"
  }
}
  
  
