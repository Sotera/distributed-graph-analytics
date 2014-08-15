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
package com.soteradefense.dga.graphx.hbse

import java.util.Date

import org.apache.spark.broadcast._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.Ordering
import scala.reflect.ClassTag


object HBSECore extends Logging with Serializable {

  private var hbseConf: HBSEConf = new HBSEConf()
  private var previousPivots: mutable.Set[VertexId] = new mutable.HashSet[VertexId]

  implicit def orderedBetweennessSet(implicit ord1: Ordering[Long], ord2: Ordering[Double]): Ordering[(Long, Double)] =
    new Ordering[(Long, Double)] {
      def compare(x: (Long, Double), y: (Long, Double)): Int = {
        val (vertexIdX, betweennessX) = x
        val (vertexIdY, betweennessY) = y
        // _2 is the betweenness value
        // _1 is the vertex id
        // This is for the ordered set of betweenness values.
        // If their values are equal, compare their Ids to make sure they are not the same node.
        if (betweennessX.equals(betweennessY)) {
          vertexIdX.compareTo(vertexIdY)
        } else {
          -betweennessX.compareTo(betweennessY)
        }
      }
    }


  def hbse[VD: ClassTag, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED]): (RDD[(Long, Double)], Graph[VertexData, Long]) = {
    hbseConf = new HBSEConf(sc.getConf)
    previousPivots = new mutable.HashSet[VertexId]
    var previousBetweennessSet: mutable.Set[(Long, Double)] = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var newlyComputedBetweennessSet: mutable.Set[(Long, Double)] = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var setDifferenceCount: Int = 0
    var keepRunning: Boolean = true
    var betweennessSetStabilityCutOff: Int = 0
    // Create an HBSE Graph
    var hbseGraph: Graph[VertexData, Long] = createHBSEGraph(graph).cache()
    // Calculate this once, so we don't have to do it every run.
    val totalNumberOfVertices = hbseGraph.vertices.count()
    do {
      betweennessSetStabilityCutOff = 0
      logInfo("Selecting Pivots")
      // Select the nodes that will send the initial messages.
      val pivots: Broadcast[mutable.Set[VertexId]] = selectPivots(sc, hbseGraph, totalNumberOfVertices)
      logInfo("Shortest Path Phase")
      hbseGraph = shortestPathRun(hbseGraph, pivots, sc)
      logInfo("Ping Predecessors and Find Successors")
      hbseGraph = pingPredecessorsAndFindSuccessors(hbseGraph, sc)
      logInfo("Get High Betweenness List")
      hbseGraph = computeHighBetweenness(hbseGraph)
      newlyComputedBetweennessSet = getHighBetweennessSet(hbseGraph)
      setDifferenceCount = compareHighBetweennessSets(previousBetweennessSet, newlyComputedBetweennessSet)
      // Close the running betweenness map and set it equal to the current set.
      previousBetweennessSet.clear()
      previousBetweennessSet = newlyComputedBetweennessSet.clone()
      // Decided if the algorithm needs to run another pass.
      val numberOfPivotsSelected = previousPivots.size + pivots.value.size
      previousPivots ++= pivots.value
      val shouldKeepRunningResult: (Boolean, Int) = shouldKeepRunning(setDifferenceCount, betweennessSetStabilityCutOff, numberOfPivotsSelected, totalNumberOfVertices)
      keepRunning = shouldKeepRunningResult._1
      betweennessSetStabilityCutOff = shouldKeepRunningResult._2

    } while (keepRunning)
    // Create an RDD to write the High Betweenness Set.
    val betweennessVertices = sc.parallelize(newlyComputedBetweennessSet.toSeq)
    (betweennessVertices, hbseGraph)
  }

  def shouldKeepRunning(delta: Int, stabilityCutOffMetCount: Int, numberOfPivotsSelected: Int, totalNumberOfVertices: Long) = {
    if (delta <= hbseConf.setStability) {
      val increaseStabilityCount = stabilityCutOffMetCount + 1
      if (increaseStabilityCount >= hbseConf.setStabilityCounter) {
        (false, stabilityCutOffMetCount + 1)
      }
      else if (numberOfPivotsSelected >= totalNumberOfVertices) {
        (false, increaseStabilityCount)
      }
      else {
        (true, increaseStabilityCount)
      }
    }
    else if (numberOfPivotsSelected >= hbseConf.vertexCount) {
      (false, stabilityCutOffMetCount)
    }
    else {
      (true, stabilityCutOffMetCount)
    }
  }

  def getHighBetweennessSet(graph: Graph[VertexData, Long]) = {
    // Build the new high betweenness set by joining and merging the old with the new.
    val newSet = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    newSet ++ graph.vertices.map(vertex => {
      val (vertexId, vertexData) = vertex
      (vertexId, vertexData.getApproximateBetweenness)
    }).takeOrdered(hbseConf.betweennessSetMaxSize)(orderedBetweennessSet)
  }

  def compareHighBetweennessSets(x: mutable.Set[(Long, Double)], y: mutable.Set[(Long, Double)]) = {
    // Compare the new set with the previous set by comparing the set members
    y.foldLeft(0)((total: Int, item: (Long, Double)) => {
      if (!(x.count(pred => pred._1 == item._1) == 1)) total + 1
      else total
    })
  }

  def computeHighBetweenness(graph: Graph[VertexData, Long]) = {
    var hbseGraph = graph.cache()
    val prevG = hbseGraph
    // Calculate your approx. betweenness based on the dependency accumulation chain.
    hbseGraph = hbseGraph.mapVertices((vertexId, vertexData) => {
      def computeBetweenness(total: Double, item: (Long, PartialDependency)) = {
        val partialDependency = item._2
        total + partialDependency.getDependency
      }
      var approxBetweenness = vertexData.getApproximateBetweenness
      approxBetweenness = vertexData.getPartialDependencyMap.foldLeft(approxBetweenness)(computeBetweenness)

      vertexData.getPartialDependencyMap.clear()
      vertexData.getPathDataMap.clear()
      new VertexData(approxBetweenness)
    }).cache()
    // Persist the new state in memory
    hbseGraph.vertices.count()
    // Unpersist the old state
    prevG.unpersistVertices(blocking = false)
    prevG.edges.unpersist(blocking = false)
    hbseGraph
  }

  def shortestPathRun(graph: Graph[VertexData, Long], pivots: Broadcast[mutable.Set[VertexId]], sc: SparkContext) = {
    var shortestPathPhasesCompleted = 0
    // Initialize the Pivots with a pathdata containing their own vertex
    var hbseGraph = graph.mapVertices((vertexId, vertexData) => {
      if (pivots.value.contains(vertexId)) {
        vertexData.addPathData(PathData.createShortestPathMessage(vertexId, vertexId, 0, 1L))
        (vertexData, -1)
      }
      else {
        (vertexData, 0)
      }
    }).cache()

    do {
      // Create a small graph of the pivots and their edges to send the initial messages.

      def accumulatePathData(vertexId: VertexId, vertexStorage: (VertexData, Int), messages: mutable.Map[Long, PathData]) = {
        //Stores the Paths that were updated
        //Process Incoming Messages
        val (graphData, updateCount) = vertexStorage
        def pathUpdateAccumulation(updateCount: Int, item: (Long, PathData)) = {
          //Add the PathData to the current vertex
          val pathData = item._2
          val updatedPath = graphData.addPathData(pathData)
          if (updatedPath != null)
            updateCount + 1
          else
            updateCount

        }
        // Process each message one at a time.  Add any updates to the updated path map.
        if (messages.nonEmpty) {
          (graphData, messages.foldLeft(0)(pathUpdateAccumulation))
        }
        else {
          vertexStorage
        }
      }
      def sendShortestPathMessage(triplet: EdgeTriplet[(VertexData, Int), Long]) = {
        val (graphData, updateCount) = triplet.srcAttr
        if (updateCount > 0) {
          val singleMap: mutable.Map[Long, PathData] = new mutable.HashMap[Long, PathData]
          val updatedPathMap = graphData.getPathDataMap
          def buildShortestPathMessage(map: mutable.Map[Long, PathData], item: (Long, ShortestPathList)) = {
            val (messageSource,shortestPathList) = item
            map.put(messageSource, PathData.createShortestPathMessage(messageSource, triplet.srcId, shortestPathList.getDistance + triplet.attr, shortestPathList.getShortestPathCount))
            map
          }
          logInfo(s"Sending ShortestPath Update Message to ${triplet.dstId} from ${triplet.srcId}")
          Iterator((triplet.dstId, updatedPathMap.foldLeft(singleMap)(buildShortestPathMessage)))
        }
        else if (updateCount == -1) {
          val singleMessageMap = new mutable.HashMap[VertexId, PathData]
          // Send a Shortest Path Message to my neighbor.
          singleMessageMap.put(triplet.srcId, PathData.createShortestPathMessage(triplet.srcId, triplet.srcId, triplet.attr, 1))
          logInfo(s"Sending ShortestPath Message to ${triplet.dstId} from ${triplet.srcId}")
          // Needs to be a list because all PathData messages need to be sent to the node from every possible pivot.
          Iterator((triplet.dstId, singleMessageMap))
        }
        else {
          Iterator.empty
        }
      }
      val prevG = hbseGraph
      hbseGraph = Pregel(hbseGraph, mutable.Map.empty[Long, PathData], activeDirection = EdgeDirection.Out)(accumulatePathData, sendShortestPathMessage, mergeMapMessage).cache()

      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)



      // Increase the Number of Completed Phases
      shortestPathPhasesCompleted += 1
    } while (!(shortestPathPhasesCompleted == hbseConf.shortestPathPhases))

    hbseGraph.mapVertices((vertexId, vertexData) => {
      val (graphData, updateCount) = vertexData
      graphData
    }
    ).cache()
  }

  def pingPredecessorsAndFindSuccessors(graph: Graph[VertexData, Long], sc: SparkContext) = {
    var hbseGraph = graph.cache()
    // Send a message to all of your predecessors that sent you shortest path messages.
    val pingRDD = hbseGraph.mapReduceTriplets(sendPingMessage, merge[Long]).cache()

    logInfo("Processing Nodes with No Successors")
    // Temp graph for processing ping messages
    val mergedGraph = hbseGraph.outerJoinVertices(pingRDD)((vertexId, vertexData, pingMessages) => {
      val validMessages = pingMessages.getOrElse(List.empty)
      validMessages.foreach(source => {
        logInfo(s"Adding Partial Dependency for: $source")
        vertexData.addPartialDependency(source, 0.0, 1)
      })
      def noSuccessorAccumulation(set: mutable.HashSet[Long], item: (Long, ShortestPathList)) = {
        val vertexIdInMyPath = item._1
        // Add all vertices in your path that didn't send you a message.
        if (vertexIdInMyPath != vertexId && !validMessages.contains(vertexIdInMyPath))
          set += vertexIdInMyPath
        set
      }
      val noSuccessor = vertexData.getPathDataMap.foldLeft(new mutable.HashSet[Long])(noSuccessorAccumulation)
      logInfo(s"No Successor Count is: ${noSuccessor.size}")
      // Build a graph of no successors and vertex data
      (noSuccessor, vertexData)
    }).cache()

    logInfo("Sending Dependency Messages")
    // Send a message to your predecessor that n number of nodes are dependent on you.
    val msgRDD = mergedGraph.mapReduceTriplets(sendDependencyMessage, merge[(Long, Double, Long)]).cache()

    // Rebuild the hbseGraph with the updated state of the vertex values
    var runGraph = mergedGraph.outerJoinVertices(msgRDD)((vertexId, vertexData, msgs) => {
      val originalVertexData = vertexData._2
      (originalVertexData, msgs.getOrElse(List.empty[(Long, Double, Long)]))
    }).cache()

    runGraph.vertices.count()


    // Pair Dependency Run State
    def vProg(vertexId: VertexId, vertexData: (VertexData, List[(Long, Double, Long)]), predecessorList: List[(Long, Double, Long)]) = {
      var newBuf: ListBuffer[(Long, Double, Long)] = new ListBuffer[(Long, Double, Long)]
      val (graphData, oldMessages) = vertexData
      def partialDependencyCalculation(accumulatedDependencies: ListBuffer[(Long, Double, Long)], partialDependency: (Long, Double, Long)) = {
        val messageSource = partialDependency._1
        if (messageSource != vertexId) {
          try {
            // Calculates your dependency
            val successorDep = partialDependency._2
            val successorNumberOfPaths = partialDependency._3
            val numPaths = graphData.getPathDataMap.get(messageSource).get.getShortestPathCount
            val partialDep = (numPaths.toDouble / successorNumberOfPaths.toDouble) * (1 + successorDep)
            val partialSum = graphData.addPartialDependency(messageSource, partialDep, -1)
            // If you're done processing all of your predecessors, then you can add a forwarded message to your predecessors.
            if (partialSum.getSuccessors == 0) {
              val listItem = Tuple3(messageSource, partialSum.getDependency, numPaths)
              accumulatedDependencies += listItem
            }
          }
          catch {
            case nse: NoSuchElementException => {
              logInfo(s"$messageSource was not found in $vertexId map")
              for ((k, v) <- graphData.getPathDataMap) {
                logInfo(s"$k is in $vertexId map")
              }
            }
            case e: Exception => throw e
          }
        }
        accumulatedDependencies
      }
      // Process all incoming messages until your number of successors reaches zero, then forward your dependency back to your predecessor
      if (predecessorList != null)
        newBuf = predecessorList.foldLeft(newBuf)(partialDependencyCalculation)

      else
        newBuf = oldMessages.foldLeft(newBuf)(partialDependencyCalculation)

      (graphData, newBuf.toList)
    }

    def sendDependencyCalculationMessages(triplet: EdgeTriplet[(VertexData, List[(Long, Double, Long)]), Long]) = {
      var buffer = new ListBuffer[(Long, Double, Long)]
      val (graphData, forwardMessages) = triplet.dstAttr
      def dependencyMessageAccumulation(buf: ListBuffer[(Long, Double, Long)], item: (Long, Double, Long)) = {
        val messageSource = item._1
        val shortestPathList = graphData.getPathDataMap.get(messageSource).get
        if (shortestPathList.getPredecessorPathCountMap.contains(triplet.srcId)) {
          val dependencyMessage = item
          buf += dependencyMessage
        }
        buf
      }
      // Sends your dependency to your predecessors who send you messages
      buffer = forwardMessages.foldLeft(buffer)(dependencyMessageAccumulation)
      if (buffer.size > 0) {
        Iterator((triplet.srcId, buffer.toList))
      }
      else {
        Iterator.empty
      }
    }
    val initialValue: List[(Long, Double, Long)] = null
    runGraph = Pregel(runGraph, initialValue, activeDirection = EdgeDirection.In)(vProg, sendDependencyCalculationMessages, merge[(Long, Double, Long)]).cache()

    val oldGraph = hbseGraph
    hbseGraph = runGraph.mapVertices((vertexId, vertexData) => {
      val (graphData, listOfMessages) = vertexData
      graphData
    }).cache()
    hbseGraph.vertices.count()

    // Unpersist the previous and merged graph because they are no longer needed.
    mergedGraph.unpersistVertices(blocking = false)
    mergedGraph.edges.unpersist(blocking = false)
    oldGraph.unpersistVertices(blocking = false)
    oldGraph.edges.unpersist(blocking = false)
    msgRDD.unpersist(blocking = false)
    pingRDD.unpersist(blocking = false)
    runGraph.unpersistVertices(blocking = false)
    runGraph.edges.unpersist(blocking = false)

    hbseGraph
  }


  def sendDependencyMessage(triplet: EdgeTriplet[(mutable.HashSet[Long], VertexData), Long]) = {
    var buffer = new ListBuffer[(Long, Double, Long)]
    val noSuccessorsList = triplet.dstAttr._1
    val vertexData = triplet.dstAttr._2
    def noSuccessorMessageAccumulation(buf: ListBuffer[(Long, Double, Long)], noSuccessorSrc: Long) = {
      val spl = vertexData.getPathDataMap.get(noSuccessorSrc).get
      if (spl.getPredecessorPathCountMap.contains(triplet.srcId)) {
        val numPaths = spl.getShortestPathCount
        val dep = 0.0
        logInfo(s"Sending Dependency Message to: ${triplet.srcId}")
        val dependencyMessage = Tuple3(noSuccessorSrc, dep, numPaths)
        buf += dependencyMessage
      }
      buf
    }
    buffer = noSuccessorsList.foldLeft(buffer)(noSuccessorMessageAccumulation)
    Iterator((triplet.srcId, buffer.toList))
  }

  def sendPingMessage(triplet: EdgeTriplet[VertexData, Long]) = {
    var buffer = new ListBuffer[Long]
    logInfo(s"About to Ping ${triplet.srcId} Predecessors")
    def buildPingMessages(buf: ListBuffer[Long], item: (Long, ShortestPathList)) = {
      val shortestPathList = item._2
      val pingVertexId = item._1
      if (shortestPathList.getDistance > 0 && shortestPathList.getPredecessorPathCountMap.contains(triplet.srcId)) {
        buf += pingVertexId
      }
      buf
    }
    buffer = triplet.dstAttr.getPathDataMap.foldLeft(buffer)(buildPingMessages)
    Iterator((triplet.srcId, buffer.toList))
  }

  def selectPivots(sc: SparkContext, hbseGraph: Graph[VertexData, Long], vertexCount: Long) = {
    var totalNumberOfPivotsUsed = previousPivots.size
    var pivots: mutable.Set[VertexId] = new mutable.HashSet[VertexId]
    def buildSetOfPivots(runningSet: mutable.HashSet[VertexId], item: (VertexId, VertexData)) = {
      val vertexId = item._1
      if (!previousPivots.contains(vertexId)) {
        runningSet += vertexId
      }
      runningSet
    }
    while (pivots.size < hbseConf.pivotBatchSize && totalNumberOfPivotsUsed < vertexCount) {
      pivots ++= hbseGraph.vertices.takeSample(withReplacement = false, hbseConf.pivotBatchSize, (new Date).getTime).foldLeft(new mutable.HashSet[VertexId])(buildSetOfPivots)
      totalNumberOfPivotsUsed = previousPivots.size + pivots.size
    }
    logInfo("Pivot Selection Done")
    sc.broadcast(pivots)
  }


  def createHBSEGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexData, Long] = {
    graph.mapVertices((vid, vd) => new VertexData()).mapEdges(e => if (e.attr == 0) 1L else e.attr.toString.toLong)
  }

  def merge[T: ClassTag](leftList: List[T], rightList: List[T]) = {
    leftList ++ rightList
  }

  def mergeMapMessage(leftMessageMap: mutable.Map[Long, PathData], rightMessageMap: mutable.Map[Long, PathData]) = {
    def mergeMapMessages(mergedMessageMap: mutable.Map[Long, PathData], item: (Long, PathData)) = {
      val key = item._1
      val value = item._2
      if (mergedMessageMap.contains(key)) {
        val bPathData = mergedMessageMap.get(key).get
        if (value.getDistance < bPathData.getDistance) {
          mergedMessageMap.put(key, value)
        }
      }
      else {
        mergedMessageMap.put(key, value)
      }
      mergedMessageMap
    }
    leftMessageMap.foldLeft(rightMessageMap)(mergeMapMessages)
  }
}
