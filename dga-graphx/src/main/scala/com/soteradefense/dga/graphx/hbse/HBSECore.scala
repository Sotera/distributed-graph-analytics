package com.soteradefense.dga.graphx.hbse

import java.util.Date

import org.apache.spark.broadcast._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, Logging, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import scala.math.Ordering
import scala.reflect.ClassTag


object HBSECore extends Logging with Serializable {

  private var hbseConf: HBSEConf = new HBSEConf()
  private var previousPivots = new mutable.HashSet[VertexId]

  implicit def orderedBetweennessSet(implicit ord1: Ordering[Long], ord2: Ordering[Double]): Ordering[(Long, Double)] =
    new Ordering[(Long, Double)] {
      def compare(x: (Long, Double), y: (Long, Double)): Int = {
        if (x._2.equals(y._2)) {
          x._1.compareTo(y._1)
        } else {
          y._2.compareTo(x._2)
        }
      }
    }

  def hbse[VD: ClassTag, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED]): (RDD[(Long, Double)], Graph[VertexData, Long]) = {
    hbseConf = new HBSEConf(sc.getConf)
    previousPivots = new mutable.HashSet[VertexId]
    var currentBetweennessMap: Set[(Long, Double)] = new immutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var runningBetweennessMap: Set[(Long, Double)] = new immutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var delta = 0
    var keepRunning = true
    var stabilityCutOffMetCount = 0
    var hbseGraph: Graph[VertexData, Long] = createHBSEGraph(graph)
    do {
      stabilityCutOffMetCount = 0
      logInfo("Selecting Pivots")
      val pivots = selectPivots(sc, hbseGraph)
      logInfo("Shortest Path Phase")
      hbseGraph = shortestPathRun(hbseGraph, pivots, sc)
      logInfo("Ping Predecessors and Find Successors")
      hbseGraph = pingPredecessorsAndFindSuccessors(hbseGraph, sc)
      logInfo("Get High Betweenness List")
      hbseGraph = computeHighBetweenness(hbseGraph)
      runningBetweennessMap = getHighBetweennessSet(hbseGraph, runningBetweennessMap)
      delta = compareHighBetweennessSets(currentBetweennessMap, runningBetweennessMap)
      currentBetweennessMap = runningBetweennessMap
      val numberOfPivotsSelected = previousPivots.size + pivots.value.size
      previousPivots ++= pivots.value
      val shouldKeepRunningTupleResult = shouldKeepRunning(delta, stabilityCutOffMetCount, numberOfPivotsSelected)
      stabilityCutOffMetCount = shouldKeepRunningTupleResult._2
      keepRunning = shouldKeepRunningTupleResult._1
      if (numberOfPivotsSelected == hbseGraph.vertices.count()) keepRunning = false
    } while (keepRunning)
    // Create an RDD to write the High Betweenness Set.
    val betweennessVertices = sc.parallelize(runningBetweennessMap.toSeq)
    val finalGraph = hbseGraph.outerJoinVertices(betweennessVertices)((vid, vdata, betweenness) => {
      vdata.setApproxBetweenness(betweenness.getOrElse(vdata.getApproximateBetweenness))
      vdata
    }).cache()
    (betweennessVertices, finalGraph)
  }

  def shouldKeepRunning(delta: Int, stabilityCutOffMetCount: Int, numberOfPivotsSelected: Int) = {
    if (delta <= hbseConf.setStability) {
      if ((stabilityCutOffMetCount + 1) >= hbseConf.setStabilityCounter) {
        (false, stabilityCutOffMetCount + 1)
      }
      else {
        (true, stabilityCutOffMetCount + 1)
      }
    }
    else if (numberOfPivotsSelected >= hbseConf.vertexCount) {
      (false, stabilityCutOffMetCount)
    }
    else {
      (true, stabilityCutOffMetCount)
    }
  }

  def getHighBetweennessSet(graph: Graph[VertexData, Long], runningBetweennessSet: Set[(Long, Double)]) = {
    new immutable.TreeSet[(Long, Double)]()(orderedBetweennessSet) ++ (runningBetweennessSet ++ graph.vertices.map(f => (f._1, f._2.getApproximateBetweenness)).takeOrdered(hbseConf.betweennessSetMaxSize)(orderedBetweennessSet))
      .groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
  }

  def compareHighBetweennessSets(x: Set[(Long, Double)], y: Set[(Long, Double)]) = {
    y.map(f => f._1).diff(x.map(g => g._1)).size
  }

  def computeHighBetweenness(graph: Graph[VertexData, Long]) = {
    val hbseGraph = graph.cache()

    val betweennessGraph = hbseGraph.mapVertices((vid, vdata) => {
      if (vdata.getPartialDependencyMap.nonEmpty) {
        var approxBetweenness = vdata.getApproximateBetweenness
        approxBetweenness = vdata.getPartialDependencyMap.map(f => f._2.getDependency).reduce((a, b) => a + b)
        vdata.setApproxBetweenness(approxBetweenness)
      }
      vdata.getPartialDependencyMap.clear()
      vdata.getPathDataMap.clear()
      vdata
    }).cache()
    hbseGraph.unpersistVertices(blocking = false)
    betweennessGraph
  }

  def shortestPathRun(graph: Graph[VertexData, Long], pivots: Broadcast[mutable.HashSet[VertexId]], sc: SparkContext) = {
    var shortestPathPhasesCompleted = 0
    val hbseGraph = graph.cache()
    var messageRDD: VertexRDD[List[PathData]] = null
    do {

      // Shortest Path Message Sends
      messageRDD = hbseGraph.mapReduceTriplets(triplet => {
        if (pivots.value.contains(triplet.srcId)) {
          // Add a PathData to my node.
          val hashMap = new mutable.HashMap[VertexId, List[PathData]]
          var pathDataBuilder = new ListBuffer[PathData]
          // Send a Shortest Path Message to my neighbor.
          pathDataBuilder += PathData.createShortestPathMessage(triplet.srcId, triplet.srcId, triplet.attr, 1)
          hashMap.put(triplet.dstId, pathDataBuilder.toList)
          logInfo(s"Sending ShortestPath Message to ${triplet.dstId} from ${triplet.srcId}")
          // Needs to be a list because all PathData messages need to be sent to the node.
          hashMap.iterator
        }
        else {
          Iterator.empty
        }
      }, mergePathDataMessage).cache()

      var updateCount: Accumulator[Int] = null

      // Shortest Path Run
      do {
        updateCount = sc.accumulator(0)(SparkContext.IntAccumulatorParam)
        // Join the HBSEGraph with the VertexRDD to Process the Messages
        val updatedPaths = hbseGraph.outerJoinVertices(messageRDD)((vid, vdata, shortestPathMessages) => {
          //Stores the Paths that were updated
          val updatedPathMap = new mutable.HashMap[Long, ShortestPathList]
          //Process Incoming Messages
          if (pivots.value.contains(vid) && !vdata.getPathDataMap.contains(vid))
            vdata.addPathData(PathData.createShortestPathMessage(vid, vid, 0, 1L))
          if (shortestPathMessages != None) {
            shortestPathMessages.get.foreach(pd => {
              //Add the PathData to the current vertex
              val updatedPath = vdata.addPathData(pd)
              if (updatedPath != null) {
                //If it updated, add it to the updatedPathMap
                logInfo(s"Path Updated ${pd.getMessageSource} for Vertex: $vid")
                updatedPathMap.put(pd.getMessageSource, updatedPath)
                updateCount += 1
              }
            })
          }
          updatedPathMap
        }).cache()

        //Needed to Persist the update count for some reason
        //        updatedPaths.vertices.count()
        updatedPaths.triplets.count()
        // Get the update count based on the size of each hashmap
        //updateCount = updatedPaths.vertices.map(verticesWithUpdatedPaths => verticesWithUpdatedPaths._2.size).reduce((a, b) => a + b)
        logInfo(s"Update Count is: $updateCount")
        //Forward the updated paths to the next edge
        messageRDD.unpersist(blocking = false)
        messageRDD = updatedPaths.mapReduceTriplets(sendShortestPathRunMessage, mergePathDataMessage).cache()
      } while (!(updateCount.value == 0))
      messageRDD.unpersist(blocking = false)
      // Increase the Number of Completed Phases
      shortestPathPhasesCompleted += 1
    } while (!(shortestPathPhasesCompleted == hbseConf.shortestPathPhases))
    hbseGraph
  }

  def pingPredecessorsAndFindSuccessors(graph: Graph[VertexData, Long], sc: SparkContext) = {
    var hbseGraph = graph.cache()
    //Ping Predecessor
    var pingRDD = hbseGraph.mapReduceTriplets(sendPingMessage, mergePathDataMessage).cache()

    logInfo("Processing Nodes with No Successors")
    val mergedGraph = hbseGraph.outerJoinVertices(pingRDD)((vid, vdata, msgs) => {
      val successorExists = new mutable.HashSet[Long]
      if (msgs != None) {
        msgs.get.foreach(pd => {
          successorExists += pd.getMessageSource
          logInfo(s"Adding Partial Dependency for: ${pd.getMessageSource}")
          vdata.addPartialDependency(pd.getMessageSource, 0.0, 1)
        })
      }
      var allPaths = vdata.getPathDataMap.keySet
      allPaths = allPaths.filter(_ != vid)
      val noSuccessor = new mutable.HashSet[Long]
      allPaths.foreach(pdKey => {
        if (!successorExists.contains(pdKey)) {
          noSuccessor += pdKey
        }
      })
      logInfo(s"No Successor Count is: ${noSuccessor.size}")
      (noSuccessor, vdata)
    }).cache()

    logInfo("Sending Dependency Messages")
    // Find Successors
    pingRDD.unpersist(blocking = false)
    pingRDD = mergedGraph.mapReduceTriplets(sendDependencyMessage, mergePathDataMessage).cache()
    var updateCount: Accumulator[Int] = null
    hbseGraph = mergedGraph.mapVertices((vid, vdata) => vdata._2).cache()
    // Pair Dependency Run State
    do {
      updateCount = sc.accumulator(0)(SparkContext.IntAccumulatorParam)
      val partialDepGraph = hbseGraph.outerJoinVertices(pingRDD)((vid, vdata, predList) => {
        var buffer = new ListBuffer[(Long, ShortestPathList, PartialDependency, Long)]
        var newBuf = new ListBuffer[(Boolean, PathData, ShortestPathList)]
        if (predList != None) {
          predList.get.filter(f => f.getMessageSource != vid).foreach(pd => {
            val successorDep = pd.getDependency
            val successorNumberOfPaths = pd.getNumberOfShortestPaths
            val numPaths = vdata.getPathDataMap.get(pd.getMessageSource).get.getShortestPathCount
            val partialDep = (numPaths.toDouble / successorNumberOfPaths.toDouble) * (1 + successorDep)
            val partialSum = vdata.addPartialDependency(pd.getMessageSource, partialDep, -1)
            val depMessage = PathData.createDependencyMessage(pd.getMessageSource, partialSum.getDependency, numPaths)
            val listItem = (partialSum.getSuccessors == 0, depMessage, vdata.getPathDataMap.get(pd.getMessageSource).get)
            newBuf += listItem
          })
        }
        newBuf.toList
      }).cache()

      pingRDD.unpersist(blocking = false)
      pingRDD = partialDepGraph.mapReduceTriplets(triplets => {
        val messageMap = new mutable.HashMap[Long, List[PathData]]
        triplets.dstAttr.foreach(item => {
          val successorsHitZero = item._1
          val messageToForward = item._2
          val spl = item._3
          if (successorsHitZero) {
            if (spl.getPredecessorPathCountMap.contains(triplets.srcId)) {
              updateCount += 1
              if (!messageMap.contains(triplets.srcId))
                messageMap.put(triplets.srcId, new ListBuffer[PathData].toList)
              val updatedList = messageMap.get(triplets.srcId).get :+ messageToForward
              messageMap.put(triplets.srcId, updatedList)
            }
          }
        })
        messageMap.iterator
      }, mergePathDataMessage).cache()

      pingRDD.count()
    } while (!(updateCount.value == 0))
    hbseGraph
  }

  //TODO: May not be needed
  def sendPairDependencyRunMessage(triplets: EdgeTriplet[(List[(Boolean, PathData, ShortestPathList)]), Long]) = {
    val messageMap = new mutable.HashMap[Long, List[PathData]]
    triplets.dstAttr.foreach(item => {
      val successorsHitZero = item._1
      val messageToForward = item._2
      val spl = item._3
      if (successorsHitZero) {
        if (spl.getPredecessorPathCountMap.contains(triplets.srcId)) {
          if (!messageMap.contains(triplets.srcId))
            messageMap.put(triplets.srcId, new ListBuffer[PathData].toList)
          val updatedList = messageMap.get(triplets.srcId).get :+ messageToForward
          messageMap.put(triplets.srcId, updatedList)
        }
      }
    })

    messageMap.iterator
  }

  def sendDependencyMessage(triplet: EdgeTriplet[(mutable.HashSet[Long], VertexData), Long]) = {
    val messageMap = new mutable.HashMap[Long, List[PathData]]
    val noSuccessorsList = triplet.dstAttr._1
    val vertexData = triplet.dstAttr._2
    if (noSuccessorsList.size > 0) {
      noSuccessorsList.foreach(noSuccessorSrc => {
        val spl = vertexData.getPathDataMap.get(noSuccessorSrc).get
        val numPaths = spl.getShortestPathCount
        val dep = 0.0
        if (spl.getPredecessorPathCountMap.contains(triplet.srcId)) {
          if (!messageMap.contains(triplet.srcId))
            messageMap.put(triplet.srcId, new ListBuffer[PathData].toList)
          logInfo(s"Sending Dependency Message to: ${triplet.srcId}")
          val dependencyMessage = PathData.createDependencyMessage(noSuccessorSrc, dep, numPaths)
          val updatedList = messageMap.get(triplet.srcId).get :+ dependencyMessage
          messageMap.put(triplet.srcId, updatedList)
        }
      })
    }
    messageMap.iterator
  }

  def sendPingMessage(triplet: EdgeTriplet[VertexData, Long]) = {
    val messageMap = new mutable.HashMap[Long, List[PathData]]
    logInfo(s"About to Ping ${triplet.srcId} Predecessors")
    for ((source, shortestPathList) <- triplet.dstAttr.getPathDataMap) {
      val distance = shortestPathList.getDistance
      if (distance > 0 && shortestPathList.getPredecessorPathCountMap.contains(triplet.srcId)) {
        // Only ping this one if a message came through the node.
        // Only ping the edge you're on.
        if (!messageMap.contains(triplet.srcId))
          messageMap.put(triplet.srcId, new ListBuffer[PathData].toList)
        logInfo(s"Pinging: ${triplet.srcId}")
        val pingMessage = PathData.createPingMessage(source)
        val updatedList = messageMap.get(triplet.srcId).get :+ pingMessage
        messageMap.put(triplet.srcId, updatedList)
      }
    }
    messageMap.iterator
  }

  def selectPivots(sc: SparkContext, hbseGraph: Graph[VertexData, Long]) = {

    val vertexCount = hbseGraph.vertices.count()
    var totalNumberOfPivotsUsed = previousPivots.size
    var pivots = new mutable.HashSet[VertexId]
    while (pivots.size != hbseConf.pivotBatchSize && totalNumberOfPivotsUsed < vertexCount) {
      pivots ++= hbseGraph.vertices.takeSample(withReplacement = false, hbseConf.pivotBatchSize, (new Date).getTime).map(f => f._1).toSet.&~(previousPivots)
      totalNumberOfPivotsUsed = previousPivots.size + pivots.size
    }
    logInfo("Pivot Selection Done")
    sc.broadcast(pivots)
  }


  def createHBSEGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexData, Long] = {
    graph.mapVertices((vid, vd) => new VertexData()).mapEdges(e => if (e.attr == 0) 1L else e.attr.toString.toLong)
  }

  def sendShortestPathRunMessage(triplet: EdgeTriplet[(mutable.HashMap[Long, ShortestPathList]), Long]) = {
    var builder = new ListBuffer[PathData]
    val updatedPathMap = triplet.srcAttr
    for ((source, value) <- updatedPathMap) {
      val numPaths = value.getShortestPathCount
      val newDistance = value.getDistance + triplet.attr
      builder += PathData.createShortestPathMessage(source, triplet.srcId, newDistance, numPaths)
    }
    logInfo(s"Sending ShortestPath Update Message to ${triplet.dstId} from ${triplet.srcId}")
    Iterator((triplet.dstId, builder.toList))
  }

  def mergePathDataMessage(listA: List[PathData], listB: List[PathData]) = {
    var buffer = new ListBuffer[PathData]
    buffer ++= listA
    buffer ++= listB
    val listC = buffer.toList
    listC
  }

}
