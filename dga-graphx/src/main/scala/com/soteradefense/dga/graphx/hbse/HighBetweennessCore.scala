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
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.Ordering
import scala.reflect.ClassTag

/**
 * Core object for running High Betweenness Set Extraction.
 *
 * @param hbseConf A configuration for the run.
 * @param previousPivots List of vertices not to use as pivots.
 */
class HighBetweennessCore(val hbseConf: HBSEConf, private var previousPivots: mutable.Set[VertexId]) extends Logging with Serializable {


  /**
   * Constructor that instantiates a HighBetweennessCore object from a SparkConf
   *
   * @param conf SparkConf object.
   * @return HighBetweennessCore object.
   */
  def this(conf: SparkConf) = this(new HBSEConf(conf), new mutable.HashSet[VertexId])

  /**
   * An implicit ordering definition to order based on high betweenness value.
   * It also defines uniqueness in the set based on vertex id.
   *
   * @param ord1 Long ordering
   * @param ord2 Double ordering
   * @return
   */
  private implicit def orderedBetweennessSet(implicit ord1: Ordering[Long], ord2: Ordering[Double]): Ordering[(Long, Double)] =
    new Ordering[(Long, Double)] {
      /**
       * Compares two highbetweenness tuples
       *
       * @param x High Betweenness Node
       * @param y High Betweenness Node
       * @return The node with the highest betweenness and uniqueness based on both values.
       */
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


  /**
   * The core of the hbse algorithm.
   *
   * @param sc The current spark context
   * @param graph A graph of any type.
   * @tparam VD Type of the vertex data.
   * @tparam ED Type of the edge data.
   * @return A set of betweenness vertices and the final graph with an attached betweenness value.
   */
  def runHighBetweennessSetExtraction[VD: ClassTag, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED]): (RDD[(Long, Double)], Graph[HBSEData, Long]) = {
    var previousBetweennessSet: mutable.Set[(Long, Double)] = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var newlyComputedBetweennessSet: mutable.Set[(Long, Double)] = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var setDifferenceCount: Int = 0
    var keepRunning: Boolean = true
    var betweennessSetStabilityCutOff: Int = 0
    // Create an HBSE Graph
    var hbseGraph: Graph[HBSEData, Long] = createHBSEGraph(graph).cache()
    // Calculate this once, so we don't have to do it every run.
    val totalNumberOfVertices = hbseGraph.vertices.count()
    do {
      betweennessSetStabilityCutOff = 0
      logInfo("Selecting Pivots")
      // Select the nodes that will send the initial messages.
      val pivots: Broadcast[mutable.Set[VertexId]] = selectPivots(sc, hbseGraph, totalNumberOfVertices)
      logInfo("Shortest Path Phase")
      hbseGraph = shortestPathRun(hbseGraph, pivots)
      logInfo("Ping Predecessors and Find Successors")
      hbseGraph = pingPredecessorsAndFindSuccessors(hbseGraph)
      logInfo("Compute High Betweenness List")
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
    previousPivots.clear()
    // Create an RDD to write the High Betweenness Set.
    val betweennessVertices = sc.parallelize(newlyComputedBetweennessSet.toSeq)

    (betweennessVertices, hbseGraph)
  }

  /**
   * A method that decides if the algorithm should keep running.
   *
   * @param delta The change in the betweenness sets.
   * @param stabilityCutOffMetCount The number of times the delta met the set stability configuration.
   * @param numberOfPivotsSelected The total number of pivots selected.
   * @param totalNumberOfVertices The total number of vertices.
   * @return A boolean value of whether or not the algorithm should keep running and an updated set stability cut off count.
   */
  private def shouldKeepRunning(delta: Int, stabilityCutOffMetCount: Int, numberOfPivotsSelected: Int, totalNumberOfVertices: Long): (Boolean, Int) = {
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
    else if (numberOfPivotsSelected >= hbseConf.totalNumberOfPivots) {
      (false, stabilityCutOffMetCount)
    }
    else {
      (true, stabilityCutOffMetCount)
    }
  }

  /**
   * Gets the current high betweenness set based on the newly computed values.
   *
   * @param graph A graph with it's updated betweenness values.
   * @return A set of vertices and their betweenness values.
   */
  private def getHighBetweennessSet(graph: Graph[HBSEData, Long]): mutable.Set[(Long, Double)] = {
    // Build the new high betweenness set by joining and merging the old with the new.
    val newSet: mutable.Set[(Long, Double)] = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    newSet ++ graph.vertices.map(vertex => {
      val (vertexId, vertexData) = vertex
      (vertexId, vertexData.getApproximateBetweenness)
    }).takeOrdered(hbseConf.betweennessSetMaxSize)(orderedBetweennessSet)
  }

  /**
   * Compares two high betweenness sets and returns the difference in the sets.
   * Note: Only compares the VertexIds in the sets and not the betweenness values.
   *
   * @param x A high betweenness set
   * @param y A high betweenness set
   * @return The total difference between the two sets.
   */
  private def compareHighBetweennessSets(x: mutable.Set[(Long, Double)], y: mutable.Set[(Long, Double)]): Int = {
    // Compare the new set with the previous set by comparing the set members
    y.foldLeft(0)((total: Int, item: (Long, Double)) => {
      if (!(x.count(pred => pred._1 == item._1) == 1)) total + 1
      else total
    })
  }

  /**
   * Computes the betweenness of a graph based on it's partial dependencies.
   *
   * @param graph A graph of type VertexData and Long
   * @return A graph with it's updated betweenness value and it's maps cleared.
   */
  private def computeHighBetweenness(graph: Graph[HBSEData, Long]) = {
    var hbseGraph = graph.cache()
    val prevG = hbseGraph
    // Calculate your approx. betweenness based on the dependency accumulation chain.
    hbseGraph = hbseGraph.mapVertices((vertexId, vertexData) => {
      /**
       * Accumulates the betweenness value.
       *
       * @param total Total running betweenness value.
       * @param item A partial dependency map item.
       * @return Accumulated betweenness.
       */
      def computeBetweenness(total: Double, item: (Long, PartialDependency)) = {
        val partialDependency = item._2
        total + partialDependency.getDependency
      }
      var approxBetweenness = vertexData.getApproximateBetweenness
      approxBetweenness = vertexData.getPartialDependencyMap.foldLeft(approxBetweenness)(computeBetweenness)

      vertexData.getPartialDependencyMap.clear()
      vertexData.getPathDataMap.clear()
      new HBSEData(approxBetweenness)
    }).cache()
    // Persist the new state in memory
    hbseGraph.vertices.count()
    // Unpersist the old state
    prevG.unpersistVertices(blocking = false)
    prevG.edges.unpersist(blocking = false)
    hbseGraph
  }

  /**
   * Calculates the shortest path between every single node and the selected pivot points.
   * It accumulates these in a shortest path map.
   *
   * @param graph A hbse formatted graph.
   * @param pivots The selected pivot points for this run.
   * @return A graph with it's updated shortest paths.
   */
  private def shortestPathRun(graph: Graph[HBSEData, Long], pivots: Broadcast[mutable.Set[VertexId]]): Graph[HBSEData, Long] = {
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

      /**
       * Accumulates the path data that was sent to me.
       *
       * @param vertexId VertexId
       * @param vertexStorage My VertexData and the number of updated paths.
       * @param messages Map of new Path Data that might need updated.
       * @return Updated Vertex Data values.
       */
      def accumulatePathData(vertexId: VertexId, vertexStorage: (HBSEData, Int), messages: mutable.Map[Long, PathData]): (HBSEData, Int) = {
        //Stores the Paths that were updated
        //Process Incoming Messages
        val (graphData, updateCount) = vertexStorage
        /**
         * Updates a pathdata object in my map of path data.
         *
         * @param updateCount Accumulated total of updates.
         * @param item An item from the messages Map.
         * @return updateCount [+ 1]
         */
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

      /**
       * Sends a message to my successor if my update count > 0.
       *
       * @param triplet An edge triplet that contains the (VertexData, UpdateCount), Edge Weight.
       * @return An iterator of map messages containing path data.
       */
      def sendShortestPathMessage(triplet: EdgeTriplet[(HBSEData, Int), Long]) = {
        val (graphData, updateCount) = triplet.srcAttr
        if (updateCount > 0) {
          val singleMap: mutable.Map[Long, PathData] = new mutable.HashMap[Long, PathData]
          val updatedPathMap = graphData.getPathDataMap
          /**
           * Builds shortest path messages map from a pivot.
           *
           * @param map Running Path Map that needs to be sent to my successor.
           * @param item Path Map Item.
           * @return Map of path values.
           */
          def buildShortestPathMessage(map: mutable.Map[Long, PathData], item: (Long, ShortestPathList)) = {
            val (messageSource, shortestPathList) = item
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

  /**
   * Runs through the Ping phase and dependency accumulation phase.
   *
   * @param graph A graph with shortest path data accumulated.
   * @return A updated graph with it's dependencies accumulated.
   */
  private def pingPredecessorsAndFindSuccessors(graph: Graph[HBSEData, Long]): Graph[HBSEData, Long] = {
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
      /**
       * Accumulates the Vertices with no successors.
       * (Nodes in the path that did not send ping messages.)
       *
       * @param set A set of vertexIds that have no successor.
       * @param item A path data map item.
       * @return A set of vertexIds with no successor.
       */
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
    /**
     * Calculates a vertices dependency.
     * Note: It only forwards it's dependency if it has received all messages from it's successors and the successor count is zero.
     *
     * @param vertexId VertexId
     * @param vertexData VertexData consisting of HBSEData and List of nodes and their dependency.
     * @param predecessorList List of predecessors and their dependency.
     * @return It's vertexdata and a list of dependency for a vertex that needs to be forwarded to the predecessors.
     */
    def calculateVertexDependency(vertexId: VertexId, vertexData: (HBSEData, List[(Long, Double, Long)]), predecessorList: List[(Long, Double, Long)]) = {
      var newBuf: ListBuffer[(Long, Double, Long)] = new ListBuffer[(Long, Double, Long)]
      val (graphData, oldMessages) = vertexData
      /**
       * Calculates the partial dependency based on the messages a node has received from it's predecessors.
       *
       * @param accumulatedDependencies A list of dependencies that has been fully accumulated.
       * @param partialDependency A dependency message.
       * @return A running list of dependencies that has been fully accumulated.
       */
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
      // If predecessorList != null then it means it was send in the pregel op else it's the inital message send in the pregel op.
      if (predecessorList != null)
        newBuf = predecessorList.foldLeft(newBuf)(partialDependencyCalculation)
      else
        newBuf = oldMessages.foldLeft(newBuf)(partialDependencyCalculation)

      (graphData, newBuf.toList)
    }

    /**
     * Sends the accumulated dependency back up the path.
     *
     * @param triplet An edge triplet with (VertexData, Dependency Accumulation List) and Long Edge Value.
     * @return An iterator containing messages to forward to the srcId.
     */
    def sendDependencyCalculationMessages(triplet: EdgeTriplet[(HBSEData, List[(Long, Double, Long)]), Long]) = {
      var buffer = new ListBuffer[(Long, Double, Long)]
      val (graphData, forwardMessages) = triplet.dstAttr
      /**
       * A method to build a list of dependencies.
       *
       * @param buf A running dependency list.
       * @param item A dependency item that has been fully accumulated in the forward chain.
       * @return A dependency list buffer that is going to be sent to the srcId.
       */
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
    runGraph = Pregel(runGraph, initialValue, activeDirection = EdgeDirection.In)(calculateVertexDependency, sendDependencyCalculationMessages, merge[(Long, Double, Long)]).cache()

    val oldGraph = hbseGraph
    hbseGraph = runGraph.mapVertices((vertexId, vertexData) => {
      val (graphData, listOfMessages) = vertexData
      graphData
    }).cache()
    hbseGraph.vertices.count()

    // Release the previous and merged graph because they are no longer needed.
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


  /**
   * Sends a dependency message to the vertices you depend on.
   *
   * @param triplet An edge triplet with vertex type (HashSet[Long], VertexData) and edge
   *                type Long.
   * @return Tuple3 List Iterator sent to srcId.
   */
  private def sendDependencyMessage(triplet: EdgeTriplet[(mutable.HashSet[Long], HBSEData), Long]) = {
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

  /**
   * Sends a message to your predecessors in your path data map.
   * It only sends through valid edges who send you messages.
   *
   * @param triplet An edge triplet of type VertexData and Long.
   * @return Long List Iterator sent to the srcId.
   */
  private def sendPingMessage(triplet: EdgeTriplet[HBSEData, Long]) = {
    var buffer = new ListBuffer[Long]
    logInfo(s"About to Ping ${triplet.srcId} Predecessors")
    /**
     * Builds a Ping list of vertices that sent you a message.
     *
     * @param buf A list buffer of Long (VertexId) values.
     * @param item A path data map item.
     * @return A buffered list of valid (The src vertex sent a message to dst) ping messages.
     */
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

  /**
   * Selects pivot points for the current hbse run.
   *
   * @param sc The current SparkContext
   * @param hbseGraph An hbse graph.
   * @param vertexCount The total number of vertices in hbseGraph.
   * @return A set of pivot points stored in a broadcast variable.
   */
  private def selectPivots(sc: SparkContext, hbseGraph: Graph[HBSEData, Long], vertexCount: Long) = {
    var totalNumberOfPivotsUsed = previousPivots.size
    var pivots: mutable.Set[VertexId] = new mutable.HashSet[VertexId]
    /**
     * Builds a set of pivot points based on the previous pivot points.
     *
     * @param runningSet The running set for this particular sample.
     * @param item A vertex taken from the takeSample array.
     * @return The running set of valid pivot points.
     */
    def buildSetOfPivots(runningSet: mutable.HashSet[VertexId], item: (VertexId, HBSEData)) = {
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


  /**
   * Creates a graph, specificially for the HBSE Algorithm.
   *
   * @param graph Any type of graphx graph.
   * @tparam VD Graph vertex data type.
   * @tparam ED Graph Edge data type.
   * @return A graph specifically for the hbse algorithm.
   */
  private def createHBSEGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[HBSEData, Long] = {
    graph.mapVertices((vid, vd) => new HBSEData()).mapEdges(e => if (e.attr == 0) 1L else e.attr.toString.toLong)
  }

  /**
   * Merges two lists.
   * @param leftList The first list
   * @param rightList The second list
   * @tparam T The type of the lists
   * @return A merged leftList and rightList.
   */
  private def merge[T: ClassTag](leftList: List[T], rightList: List[T]) = {
    leftList ++ rightList
  }

  /**
   * Merges Two Maps into one map.
   *
   * @param leftMessageMap The left Map
   * @param rightMessageMap The right Map
   * @return A merged left map and right map.
   */
  private def mergeMapMessage(leftMessageMap: mutable.Map[Long, PathData], rightMessageMap: mutable.Map[Long, PathData]) = {
    /**
     * Merges one map into the other map.
     *
     * @param mergedMessageMap map being merged into.
     * @param item Map item from the other map.
     * @return Updated merged map.
     */
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
