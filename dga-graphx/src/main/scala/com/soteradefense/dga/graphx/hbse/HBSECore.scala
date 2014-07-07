package com.soteradefense.dga.graphx.hbse

import org.apache.spark.graphx._
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


  def hbse[VD: ClassTag, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED]): Graph[VertexData, Long] = {
    hbseConf = new HBSEConf(sc.getConf)
    var currentBetweennessMap = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var delta = 0
    var keepRunning = true
    var stabilityCutOffMetCount = 0
    var hbseGraph: Graph[VertexData, Long] = null
    do {
      stabilityCutOffMetCount = 0
      hbseGraph = createHBSEGraph(graph)
      logInfo("Selecting Pivots")
      // Works
      selectPivots(hbseGraph)
      logInfo("Shortest Path Phase")
      hbseGraph = shortestPathRun(hbseGraph)
      logInfo("Ping Predecessors and Find Successors")
      hbseGraph = pingPredecessorsAndFindSuccessors(hbseGraph)
      logInfo("Get High Betweenness List")
      hbseGraph = computeHighBetweenness(hbseGraph)
      val newBetweennessMap = getHighBetweennessSet(hbseGraph)
      delta = compareHighBetweennessSets(currentBetweennessMap, newBetweennessMap)
      currentBetweennessMap = newBetweennessMap
      previousPivots ++= pivots
      val shouldKeepRunningTupleResult = shouldKeepRunning(delta, stabilityCutOffMetCount)
      stabilityCutOffMetCount = shouldKeepRunningTupleResult._2
      keepRunning = shouldKeepRunningTupleResult._1
    } while (keepRunning)
    hbseGraph
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

  def getHighBetweennessSet(graph: Graph[VertexData, Long]) = {
    val betweennessQueue = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    graph.vertices.foreach(f => betweennessQueue += ((f._1, f._2.getApproximateBetweenness)))
    betweennessQueue ++ graph.vertices.map(f => (f._1, f._2.getApproximateBetweenness)).takeOrdered(hbseConf.betweennessSetMaxSize)(orderedBetweennessSet)
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
    hbseGraph
  }

  def shortestPathRun(graph: Graph[VertexData, Long]) = {
    var shortestPathPhasesCompleted = 0
    var hbseGraph = graph.cache()
    var messageRDD: VertexRDD[List[PathData]] = null
    do {
      // Shortest Path Message Sends
      messageRDD = hbseGraph.mapReduceTriplets(sendShortestPathMessage, mergePathDataMessage)
      var updateCount = 0


      // Shortest Path Run
      do {
        // Join the HBSEGraph with the VertexRDD to Process the Messages
        //val updatedPaths = hbseGraph.vertices.innerJoin(messageRDD.cache())((vid, vdata, shortestPathMessages) => {
        val updatedPaths = hbseGraph.outerJoinVertices(messageRDD.cache())((vid, vdata, shortestPathMessages) => {
          //Stores the Paths that were updated
          val updatedPathMap = new mutable.HashMap[Long, ShortestPathList]
          //Process Incoming Messages
          vdata.addPathData(PathData.createShortestPathMessage(vid, vid, 0, 1L))
          if (shortestPathMessages != None) {
            shortestPathMessages.get.foreach(pd => {
              //Add the PathData to the current vertex
              val updatedPath = vdata.addPathData(pd)
              if (updatedPath != null) {
                //If it updated, add it to the updatedPathMap
                logInfo(s"Path Updated ${pd.getSource} for Vertex: $vid")
                updatedPathMap.put(pd.getSource, updatedPath)
              }
            })
          }
          (updatedPathMap, vdata)
        }).cache()
        updateCount = updatedPaths.vertices.map(verticesWithUpdatedPaths => verticesWithUpdatedPaths._2._1.size).reduce((a, b) => a + b)
        logInfo(s"Update Count is: $updateCount")
        messageRDD = updatedPaths.mapReduceTriplets(sendShortestPathRunMessage, mergePathDataMessage)
        hbseGraph = updatedPaths.mapVertices((vid, vdata) => vdata._2)
      } while (!(updateCount == 0))

      // Increase the Number of Completed Phases
      shortestPathPhasesCompleted += 1
    } while (!(shortestPathPhasesCompleted == hbseConf.shortestPathPhases))
    hbseGraph
  }

  def pingPredecessorsAndFindSuccessors(graph: Graph[VertexData, Long]) = {
    var finalGraph: Graph[VertexData, Long] = null
    var hbseGraph = graph.cache()
    // Makes it work.  Wat..
    hbseGraph.triplets.count()
    hbseGraph.vertices.count()

    var pingRDD = hbseGraph.mapReduceTriplets(sendPingMessage, mergePathDataMessage, Some((hbseGraph.vertices, EdgeDirection.Either))).cache()

    val equal = pingRDD.count() == hbseGraph.vertices.count()
    logInfo("Processing Nodes with No Successors")
    val noSuccessorsRDD = hbseGraph.outerJoinVertices(pingRDD)((vid, vdata, msgs) => {
      val successorExists = new mutable.HashSet[Long]
      if (msgs != None) {
        msgs.get.foreach(pd => {
          successorExists += pd.getSource
          logInfo(s"Adding Partial Dependency for: ${pd.getSource}")
          vdata.addPartialDependency(pd.getSource, 0.0, 1)
        })
      }
      val allPaths = vdata.getPathDataMap.keySet
      val noSuccessor = new mutable.HashSet[Long]
      allPaths.foreach(pdKey => {
        if (!successorExists.contains(pdKey) && vid != pdKey) {
          noSuccessor += pdKey
        }
      })
      logInfo(s"No Successor Count is: ${noSuccessor.size}")
      (noSuccessor, vdata)
    }).cache()
    val mergedGraph = noSuccessorsRDD

    // Makes it work.  Wat..
    mergedGraph.triplets.count()
    mergedGraph.vertices.count()
    logInfo("Sending Dependency Messages")
    pingRDD = mergedGraph.mapReduceTriplets(sendDependencyMessage, mergePathDataMessage, Some((hbseGraph.vertices, EdgeDirection.Either)))
    var updateCount = 0

    hbseGraph = mergedGraph.mapVertices((vid, vdata) => vdata._2).cache()
    hbseGraph.vertices.count()
    hbseGraph.triplets.count()
    do {
      val partialDepGraph = hbseGraph.outerJoinVertices(pingRDD.cache())((vid, vdata, predList) => {
        var buffer = new ListBuffer[(ShortestPathList, PartialDependency, Long)]
        if (predList != None) {
          predList.get.foreach(pd => {
            if (pd.getSource != vid) {
              val successorDep = pd.getDependency
              val successorNumberOfPaths = pd.getNumberOfShortestPaths
              val numPaths = vdata.getPathDataMap.get(pd.getSource).get.getShortestPathCount
              val partialDep = (numPaths.toDouble / successorNumberOfPaths.toDouble) * (1 + successorDep)
              val partialSum = vdata.addPartialDependency(pd.getSource, partialDep, -1)
              val listItem = (vdata.getPathDataMap.get(pd.getSource).get, partialSum, numPaths)
              buffer += listItem
            }
          })
        }
        (buffer.toList, vdata)
      }).cache()
      partialDepGraph.vertices.count()
      partialDepGraph.triplets.count()
      pingRDD = partialDepGraph.mapReduceTriplets(sendPairDependencyRunMessage, mergePathDataMessage, Some((hbseGraph.vertices, EdgeDirection.Either)))
      updateCount = partialDepGraph.vertices.map(f => {
        var sum = 0
        f._2._1.foreach(item => {
          if (item._2.getSuccessors == 0)
            sum += 1
        })
        sum
      }).reduce((a, b) => a + b)
      finalGraph = partialDepGraph.mapVertices((vid, vdata) => vdata._2).cache()
    } while (!(updateCount == 0))
    finalGraph.vertices.count()
    finalGraph.triplets.count()
    finalGraph
  }

  def sendPairDependencyRunMessage(triplets: EdgeTriplet[(List[(ShortestPathList, PartialDependency, Long)], VertexData), Long]) = {
    val messageMap = new mutable.HashMap[Long, List[PathData]]
    triplets.srcAttr._1.foreach(item => {
      val spl = item._1
      val partialD = item._2
      val numPaths = item._3
      if (partialD.getSuccessors == 0) {
        spl.getPredecessorPathCountMap.keySet.foreach(pred => {
          if (!messageMap.contains(pred))
            messageMap.put(pred, new ListBuffer[PathData].toList)
          logInfo(s"Sending Pair Dependency Message to: $pred")
          val dependencyMessage = PathData.createDependencyMessage(pred, partialD.getSuccessors, numPaths)
          val updatedList = messageMap.get(pred).get.+:(dependencyMessage)
          messageMap.put(pred, updatedList)
        })
      }
    })
    messageMap.iterator
  }

  def sendDependencyMessage(triplet: EdgeTriplet[(mutable.HashSet[Long], VertexData), Long]) = {
    if (triplet.srcAttr._1.size > 0) {
      val messageMap = new mutable.HashMap[Long, List[PathData]]
      triplet.srcAttr._1.foreach(noSuccessorSrc => {
        val spl = triplet.srcAttr._2.getPathDataMap.get(noSuccessorSrc).get
        val numPaths = spl.getShortestPathCount
        val dep = 0.0
        spl.getPredecessorPathCountMap.keySet.foreach(predecessor => {
          if (!messageMap.contains(predecessor))
            messageMap.put(predecessor, new ListBuffer[PathData].toList)
          logInfo(s"Sending Dependency Message to: $predecessor")
          val dependencyMessage = PathData.createDependencyMessage(noSuccessorSrc, dep, numPaths)
          val updatedList = messageMap.get(predecessor).get.+:(dependencyMessage)
          messageMap.put(predecessor, updatedList)
        })
      })
      triplet.srcAttr._1.clear()
      messageMap.iterator
    }
    else {
      Iterator.empty
    }
  }

  def sendPingMessage(triplet: EdgeTriplet[VertexData, Long]) = {
    val messageMap = new mutable.HashMap[Long, List[PathData]]
    logInfo(s"About to Ping ${triplet.srcId} Predecessors")
    triplet.srcAttr.getPathDataMap.foreach(shortestPathTuple => {
      val source = shortestPathTuple._1
      val distance = shortestPathTuple._2.getDistance
      if (distance > 0) {
        shortestPathTuple._2.getPredecessorPathCountMap.keySet.foreach(pred => {
          if (!messageMap.contains(pred))
            messageMap.put(pred, new ListBuffer[PathData].toList)
          logInfo(s"Pinging: $pred")
          val pingMessage = PathData.createPingMessage(source)
          val updatedList = messageMap.get(pred).get.+:(pingMessage)
          messageMap.put(pred, updatedList)
        })
      }
    })
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
    if (pivots.contains(triplet.srcId)) {
      // Add a PathData to my node.
      val hashMap = new mutable.HashMap[VertexId, List[PathData]]
      var pathDataBuilder = new ListBuffer[PathData]

      // Send a Shortest Path Message to my neighbor.
      pathDataBuilder += PathData.createShortestPathMessage(triplet.srcId, triplet.srcId, triplet.attr, 1)
      hashMap.put(triplet.dstId, pathDataBuilder.toList)
      //buffer += Tuple2(triplet.dstId, pathDataBuilder.toList)
      logInfo(s"Sending ShortestPath Message to ${triplet.dstId} from ${triplet.srcId}")
      // Needs to be a list because all PathData messages need to be sent to the node.
      //buffer.iterator
      hashMap.iterator
    }
    else {
      Iterator.empty
    }
  }

  def sendShortestPathRunMessage(triplet: EdgeTriplet[(mutable.HashMap[Long, ShortestPathList], VertexData), Long]) = {
    var builder = new ListBuffer[PathData]
    for ((source, value) <- triplet.srcAttr._1) {
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
