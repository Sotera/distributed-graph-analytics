package com.soteradefense.dga.graphx.hbse

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.Ordering
import scala.reflect.ClassTag


object HBSECore extends Logging {

  private val pivots: mutable.HashSet[Long] = new mutable.HashSet[Long]
  private val previousPivots: mutable.HashSet[Long] = new mutable.HashSet[Long]
  private var hbseConf: HBSEConf = new HBSEConf()


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
    var currentBetweennessMap = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var runningBetweennessMap = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var delta = 0
    var keepRunning = true
    var stabilityCutOffMetCount = 0
    var hbseGraph: Graph[VertexData, Long] = null
    do {
      stabilityCutOffMetCount = 0
      hbseGraph = createHBSEGraph(graph)
      logInfo("Selecting Pivots")
      selectPivots(hbseGraph)
      logInfo("Shortest Path Phase")
      hbseGraph = shortestPathRun(hbseGraph)
      logInfo("Ping Predecessors and Find Successors")
      hbseGraph = pingPredecessorsAndFindSuccessors(hbseGraph)
      logInfo("Get High Betweenness List")
      hbseGraph = computeHighBetweenness(hbseGraph)
      runningBetweennessMap = getHighBetweennessSet(hbseGraph, runningBetweennessMap)
      delta = compareHighBetweennessSets(currentBetweennessMap, runningBetweennessMap)
      currentBetweennessMap = runningBetweennessMap
      previousPivots ++= pivots
      val shouldKeepRunningTupleResult = shouldKeepRunning(delta, stabilityCutOffMetCount)
      stabilityCutOffMetCount = shouldKeepRunningTupleResult._2
      keepRunning = shouldKeepRunningTupleResult._1
      if (previousPivots.size == hbseGraph.vertices.count()) keepRunning = false
    } while (keepRunning)
    // Create an RDD to write the High Betweenness Set.
    val betweennessVertices = sc.parallelize(runningBetweennessMap.toSeq)
    // Save the running set to the respective vertices.
    hbseGraph.vertices.foreach(f => {
      f._2.setApproxBetweenness(runningBetweennessMap.find(p => p._1.equals(f._1)).getOrElse((f._1,f._2.getApproximateBetweenness))._2)
    })
    (betweennessVertices, Graph(hbseGraph.vertices, hbseGraph.edges, new VertexData()))
  }

  def shouldKeepRunning(delta: Int, stabilityCutOffMetCount: Int) = {
    if (delta <= hbseConf.setStability) {
      if ((stabilityCutOffMetCount + 1) >= hbseConf.setStabilityCounter) {
        (false, stabilityCutOffMetCount + 1)
      }
      else {
        (true, stabilityCutOffMetCount + 1)
      }
    }
    else if (previousPivots.size >= hbseConf.vertexCount) {
      (false, stabilityCutOffMetCount)
    }
    else {
      (true, stabilityCutOffMetCount)
    }
  }

  def getHighBetweennessSet(graph: Graph[VertexData, Long], runningBetweennessSet: mutable.TreeSet[(Long, Double)]) = {
    graph.vertices.foreach(f => runningBetweennessSet += ((f._1, f._2.getApproximateBetweenness)))
    runningBetweennessSet ++ graph.vertices.map(f => (f._1, f._2.getApproximateBetweenness)).takeOrdered(hbseConf.betweennessSetMaxSize)(orderedBetweennessSet)
  }

  def compareHighBetweennessSets(x: mutable.TreeSet[(Long, Double)], y: mutable.TreeSet[(Long, Double)]) = {
    y.map(f => f._1).diff(x.map(g => g._1)).size
  }

  def computeHighBetweenness(graph: Graph[VertexData, Long]) = {
    val hbseGraph = graph.cache()
    // Makes it work.  Wat..
    hbseGraph.triplets.count()
    hbseGraph.vertices.count()
    hbseGraph.vertices.foreach(f => {
      var approxBetweenness = f._2.getApproximateBetweenness
      f._2.getPartialDependencyMap.values.foreach(dep => {
        approxBetweenness += dep.getDependency
      })
      f._2.setApproxBetweenness(approxBetweenness)
      f._2.getPartialDependencyMap.clear()
      f._2.getPathDataMap.clear()
    })
    Graph(hbseGraph.vertices, hbseGraph.edges, new VertexData())
  }

  def shortestPathRun(graph: Graph[VertexData, Long]) = {
    var shortestPathPhasesCompleted = 0
    val hbseGraph = graph.cache()
    hbseGraph.triplets.count()
    hbseGraph.vertices.count()
    var messageRDD: VertexRDD[List[PathData]] = null
    do {

      // Shortest Path Message Sends
      messageRDD = hbseGraph.mapReduceTriplets(sendShortestPathMessage, mergePathDataMessage)
      var updateCount = 0


      // Shortest Path Run
      do {
        // Join the HBSEGraph with the VertexRDD to Process the Messages
        val updatedPaths = hbseGraph.outerJoinVertices(messageRDD.cache())((vid, vdata, shortestPathMessages) => {
          //Stores the Paths that were updated
          val updatedPathMap = new mutable.HashMap[Long, ShortestPathList]
          //Process Incoming Messages
          if (pivots.contains(vid))
            vdata.addPathData(PathData.createShortestPathMessage(vid, vid, 0, 1L))
          if (shortestPathMessages != None) {
            shortestPathMessages.get.foreach(pd => {
              //Add the PathData to the current vertex
              val updatedPath = vdata.addPathData(pd)
              if (updatedPath != null) {
                //If it updated, add it to the updatedPathMap
                logInfo(s"Path Updated ${pd.getMessageSource} for Vertex: $vid")
                updatedPathMap.put(pd.getMessageSource, updatedPath)
              }
            })
          }
          updatedPathMap
        }).cache()
        // Get the update count based on the size of each hashmap
        updateCount = updatedPaths.vertices.map(verticesWithUpdatedPaths => verticesWithUpdatedPaths._2.size).reduce((a, b) => a + b)
        logInfo(s"Update Count is: $updateCount")
        //Forward the updated paths to the next edge
        messageRDD = updatedPaths.mapReduceTriplets(sendShortestPathRunMessage, mergePathDataMessage)
      } while (!(updateCount == 0))

      // Increase the Number of Completed Phases
      shortestPathPhasesCompleted += 1
    } while (!(shortestPathPhasesCompleted == hbseConf.shortestPathPhases))

    Graph(hbseGraph.vertices, hbseGraph.edges, new VertexData())
  }

  def pingPredecessorsAndFindSuccessors(graph: Graph[VertexData, Long]) = {
    var hbseGraph = graph.cache()
    // Makes it work.  Wat..
    hbseGraph.vertices.count()
    hbseGraph.triplets.count()

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

    // Send the messages with the nodes that have no successor.
    mergedGraph.triplets.count()
    mergedGraph.vertices.count()
    logInfo("Sending Dependency Messages")
    // Find Successors

    //TODO: Maybe broken past here
    pingRDD = mergedGraph.mapReduceTriplets(sendDependencyMessage, mergePathDataMessage)
    var updateCount = 0
    //hbseGraph = Graph(mergedGraph.vertices.map(f => (f._1,f._2._2)).cache(), hbseGraph.edges)
    hbseGraph = mergedGraph.mapVertices((vid, vdata) => vdata._2).cache()
    hbseGraph.vertices.count()
    hbseGraph.triplets.count()
    // Pair Dependency Run State
    do {
      val partialDepGraph = hbseGraph.outerJoinVertices(pingRDD.cache())((vid, vdata, predList) => {
        var buffer = new ListBuffer[(Long, ShortestPathList, PartialDependency, Long)]
        var newBuf = new ListBuffer[(Boolean, PathData, ShortestPathList)]
        if (predList != None) {
          predList.get.foreach(pd => {
            if (pd.getMessageSource != vid) {
              val successorDep = pd.getDependency
              val successorNumberOfPaths = pd.getNumberOfShortestPaths
              val numPaths = vdata.getPathDataMap.get(pd.getMessageSource).get.getShortestPathCount
              val partialDep = (numPaths.toDouble / successorNumberOfPaths.toDouble) * (1 + successorDep)
              val partialSum = vdata.addPartialDependency(pd.getMessageSource, partialDep, -1)
              val depMessage = PathData.createDependencyMessage(pd.getMessageSource, partialSum.getDependency, numPaths)
              //val listItem = (pd.getMessageSource, vdata.getPathDataMap.get(pd.getMessageSource).get, partialSum, numPaths)
              val listItem = (partialSum.getSuccessors == 0, depMessage, vdata.getPathDataMap.get(pd.getMessageSource).get)
              newBuf += listItem
              //buffer += listItem
            }
          })
        }
        //buffer.toList
        newBuf.toList
      }).cache()
      partialDepGraph.vertices.count()
      partialDepGraph.triplets.count()
      pingRDD = partialDepGraph.mapReduceTriplets(sendPairDependencyRunMessage, mergePathDataMessage)

      updateCount = pingRDD.count().toInt

    } while (!(updateCount == 0))

    Graph(hbseGraph.vertices, hbseGraph.edges, new VertexData())
  }

  def sendPairDependencyRunMessage(triplets: EdgeTriplet[(List[(Boolean, PathData, ShortestPathList)]), Long]) = {
    //def sendPairDependencyRunMessage(triplets: EdgeTriplet[(List[(Long, ShortestPathList, PartialDependency, Long)]), Long]) = {
    val messageMap = new mutable.HashMap[Long, List[PathData]]
    triplets.dstAttr.foreach(item => {
      val successorsHitZero = item._1
      val messageToForward = item._2
      val spl = item._3
      if (successorsHitZero) {
        if (spl.getPredecessorPathCountMap.keySet.contains(triplets.srcId)) {
          if (!messageMap.contains(triplets.srcId))
            messageMap.put(triplets.srcId, new ListBuffer[PathData].toList)
          val updatedList = messageMap.get(triplets.srcId).get :+ messageToForward
          messageMap.put(triplets.srcId, updatedList)
        }
      }
    })
    //    triplets.dstAttr.foreach(item => {
    //      val src = item._1
    //      val spl = item._2
    //      val partialSum = item._3
    //      val numPaths = item._4
    //      if (partialSum.getSuccessors == 0) {
    //        if (spl.getPredecessorPathCountMap.keySet.contains(triplets.srcId)) {
    //          if (!messageMap.contains(triplets.srcId))
    //            messageMap.put(triplets.srcId, new ListBuffer[PathData].toList)
    //          logInfo(s"Sending Pair Dependency Message to: ${triplets.srcId}")
    //          val dependencyMessage = PathData.createDependencyMessage(src, partialSum.getDependency, numPaths)
    //          val updatedList = messageMap.get(triplets.srcId).get :+ dependencyMessage
    //          messageMap.put(triplets.srcId, updatedList)
    //        }
    //      }
    //    })

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
        if (spl.getPredecessorPathCountMap.keySet.contains(triplet.srcId)) {

          if (!messageMap.contains(triplet.srcId))
            messageMap.put(triplet.srcId, new ListBuffer[PathData].toList)
          logInfo(s"Sending Dependency Message to: ${triplet.srcId}")
          //val dependencyMessage = PathData.createDependencyMessage(triplet.srcId, dep, numPaths)
          val dependencyMessage = PathData.createDependencyMessage(noSuccessorSrc, dep, numPaths)
          val updatedList = messageMap.get(triplet.srcId).get :+ dependencyMessage
          messageMap.put(triplet.srcId, updatedList)
        }
      })
      //noSuccessorsList.clear()
    }
    messageMap.iterator
  }

  def sendPingMessage(triplet: EdgeTriplet[VertexData, Long]) = {
    val messageMap = new mutable.HashMap[Long, List[PathData]]
    logInfo(s"About to Ping ${triplet.srcId} Predecessors")
    for ((source, shortestPathList) <- triplet.dstAttr.getPathDataMap) {
      val distance = shortestPathList.getDistance
      if (distance > 0 && shortestPathList.getPredecessorPathCountMap.keySet.contains(triplet.srcId)) {
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

  def selectPivots(hbseGraph: Graph[VertexData, Long]) = {
    pivots.clear()
    var i = 0
    while (i != hbseConf.pivotBatchSize && (previousPivots.size + pivots.size) < hbseGraph.vertices.count()) {
      val vertex = hbseGraph.pickRandomVertex()
      if (!previousPivots.contains(vertex) && !pivots.contains(vertex)) {
        logInfo(s"$vertex was selected as a pivot")
        pivots += vertex
        i += 1
      }
    }


  }


  def createHBSEGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexData, Long] = {
    graph.mapVertices((vid, vd) => new VertexData()).mapEdges(e => if (e.attr == 0) 1L else e.attr.toString.toLong)
  }

  def sendShortestPathMessage(triplet: EdgeTriplet[VertexData, Long]) = {
    //val destAttr = triplet.otherVertexAttr(triplet.dstId)
    if (pivots.contains(triplet.srcId)) {
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
  }

  def sendShortestPathRunMessage(triplet: EdgeTriplet[(mutable.HashMap[Long, ShortestPathList]), Long]) = {
    var builder = new ListBuffer[PathData]
    val updatedPathMap = triplet.srcAttr
    //TODO: Make sure it sends to all edges.
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
