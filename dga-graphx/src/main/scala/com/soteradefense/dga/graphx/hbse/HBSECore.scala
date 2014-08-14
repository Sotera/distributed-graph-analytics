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
import org.apache.spark.{Accumulator, Logging, SparkContext}

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
        // _2 is the betweenness value
        // _1 is the vertex id
        // This is for the ordered set of betweenness values.
        // If their values are equal, compare their Ids to make sure they are not the same node.
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
    var previousBetweennessSet: mutable.Set[(Long, Double)] = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var newlyComputedBetweennessSet: mutable.Set[(Long, Double)] = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var delta: Int = 0
    var keepRunning: Boolean = true
    var stabilityCutOffMetCount: Int = 0
    // Create an HBSE Graph
    var hbseGraph: Graph[VertexData, Long] = createHBSEGraph(graph).cache()
    // Calculate this once, so we don't have to do it every run.
    val totalNumberOfVertices = hbseGraph.vertices.count()
    do {
      stabilityCutOffMetCount = 0
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
      delta = compareHighBetweennessSets(previousBetweennessSet, newlyComputedBetweennessSet)
      // Close the running betweenness map and set it equal to the current set.
      previousBetweennessSet.clear()
      previousBetweennessSet = newlyComputedBetweennessSet.clone()
      // Decided if the algorithm needs to run another pass.
      val numberOfPivotsSelected = previousPivots.size + pivots.value.size
      previousPivots ++= pivots.value
      val shouldKeepRunningResult: (Boolean, Int) = shouldKeepRunning(delta, stabilityCutOffMetCount, numberOfPivotsSelected, totalNumberOfVertices)
      keepRunning = shouldKeepRunningResult._1
      stabilityCutOffMetCount = shouldKeepRunningResult._2

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
      if (pivots.value.contains(vertexId))
        vertexData.addPathData(PathData.createShortestPathMessage(vertexId, vertexId, 0, 1L))
      vertexData
    }).cache()

    var messageRDD: VertexRDD[mutable.Map[Long, PathData]] = null
    do {
      // Create a small graph of the pivots and their edges to send the initial messages.
      val validEdges = hbseGraph.edges.filter(edgePredicate => pivots.value.contains(edgePredicate.srcId))
      val pivotGraph = Graph.fromEdges(validEdges, None)
      messageRDD = pivotGraph.mapReduceTriplets(triplet => {
        // Add a PathData to my node.
        val singleMessageMap = new mutable.HashMap[VertexId, PathData]
        // Send a Shortest Path Message to my neighbor.
        singleMessageMap.put(triplet.srcId, PathData.createShortestPathMessage(triplet.srcId, triplet.srcId, triplet.attr, 1))
        logInfo(s"Sending ShortestPath Message to ${triplet.dstId} from ${triplet.srcId}")
        // Needs to be a list because all PathData messages need to be sent to the node from every possible pivot.
        Iterator((triplet.dstId, singleMessageMap))
      }, mergeMapMessage).cache()

      // Persist the messages in memory
      messageRDD.count()

      // Unpersist the pivot graph, because it is no longer needed
      pivotGraph.unpersistVertices(blocking = false)
      pivotGraph.edges.unpersist(blocking = false)

      var updateCount: Accumulator[Int] = null

      // Shortest Path Run
      do {
        // Used for accumulating the number of shortest path updates
        updateCount = sc.accumulator(0)(SparkContext.IntAccumulatorParam)
        // Join the HBSEGraph with the VertexRDD to Process the Messages
        val updatedPaths = hbseGraph.outerJoinVertices(messageRDD)((vertexId, vertexData, shortestPathMessages) => {
          //Stores the Paths that were updated
          val fullUpdatedPathMap = new mutable.HashMap[Long, ShortestPathList]
          //Process Incoming Messages

          def pathUpdateAccumulation(updatedPathMap: mutable.HashMap[Long, ShortestPathList], item: (Long, PathData)) = {
            //Add the PathData to the current vertex
            val pathData = item._2
            val updatedPath = vertexData.addPathData(pathData)
            if (updatedPath != null) {
              //If it updated, add it to the updatedPathMap
              logInfo(s"Path Updated ${pathData.getMessageSource} for Vertex: $vertexId")
              updatedPathMap.put(pathData.getMessageSource, updatedPath)
              updateCount += 1
            }
            updatedPathMap
          }
          // Process each message one at a time.  Add any updates to the updated path map.
          (shortestPathMessages.getOrElse(Map.empty).foldLeft(fullUpdatedPathMap)(pathUpdateAccumulation), vertexData)
        }).cache()

        //Needed to Persist the update count for some reason
        //Get the update count based on the size of each hashmap
        logInfo(s"Update Count is: $updateCount")
        //Forward the updated paths to the next edge
        val prevMessages = messageRDD
        // Send all of these updates to your successors.
        messageRDD = updatedPaths.mapReduceTriplets(sendShortestPathRunMessage, mergeMapMessage).cache()

        // Persist the messages in memory
        messageRDD.count()

        // Update the hbseGraph state with each nodes new data.
        val prevG = hbseGraph
        hbseGraph = updatedPaths.mapVertices((vertexId, vertexData) => vertexData._2).cache()

        // Unpersist the old graphs and messages.
        updatedPaths.unpersistVertices(blocking = false)
        updatedPaths.edges.unpersist(blocking = false)
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
        prevMessages.unpersist(blocking = false)


      } while (!(updateCount.value == 0))
      // Increase the Number of Completed Phases
      shortestPathPhasesCompleted += 1
    } while (!(shortestPathPhasesCompleted == hbseConf.shortestPathPhases))

    hbseGraph
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
    var msgRDD = mergedGraph.mapReduceTriplets(sendDependencyMessage, merge[(Long, Double, Long)]).cache()
    // Persist the messages in memory
    msgRDD.count()
    // Rebuild the hbseGraph with the updated state of the vertex values
    var prevG = hbseGraph
    hbseGraph = mergedGraph.mapVertices((vertexId, vertexData) => {
      val originalVertexData = vertexData._2
      originalVertexData
    }).cache()
    hbseGraph.vertices.count()

    // Unpersist the previous and merged graph because they are no longer needed.
    mergedGraph.unpersistVertices(blocking = false)
    mergedGraph.edges.unpersist(blocking = false)
    prevG.unpersistVertices(blocking = false)
    prevG.edges.unpersist(blocking = false)

    var updateCount: Accumulator[Int] = null

    pingRDD.unpersist(blocking = false)
    // Pair Dependency Run State
    do {
      updateCount = sc.accumulator(0)(SparkContext.IntAccumulatorParam)
      val partialDepGraph = hbseGraph.outerJoinVertices(msgRDD)((vertexId, vertexData, predecessorList) => {
        var newBuf: ListBuffer[(Long, Double, Long)] = new ListBuffer[(Long, Double, Long)]
        def partialDependencyCalculation(accumulatedDependencies: ListBuffer[(Long, Double, Long)], partialDependency: (Long, Double, Long)) = {
          val messageSource = partialDependency._1
          if (messageSource != vertexId) {
            try {
              // Calculates your dependency
              val successorDep = partialDependency._2
              val successorNumberOfPaths = partialDependency._3
              val numPaths = vertexData.getPathDataMap.get(messageSource).get.getShortestPathCount
              val partialDep = (numPaths.toDouble / successorNumberOfPaths.toDouble) * (1 + successorDep)
              val partialSum = vertexData.addPartialDependency(messageSource, partialDep, -1)
              // If you're dont processing all of your predecessors, then you can add a forwarded message to your predecessors.
              if (partialSum.getSuccessors == 0) {
                val listItem = Tuple3(messageSource, partialSum.getDependency, numPaths)
                accumulatedDependencies += listItem
              }
            }
            catch {
              case nse: NoSuchElementException => {
                logError(s"$messageSource was not found in $vertexId map")
                for ((k, v) <- vertexData.getPathDataMap) {
                  logError(s"$k is in $vertexId map")
                }
              }
              case e: Exception => throw e
            }
          }
          accumulatedDependencies
        }
        // Process all incoming messages until your number of successors reaches zero, then forward your dependency back to your predecessor
        newBuf = predecessorList.getOrElse(List.empty).foldLeft(newBuf)(partialDependencyCalculation)
        (newBuf.toList, vertexData)
      }).cache()

      // Nodes who have received messages may start their dependency accumulation chain
      val prevMessages = msgRDD
      msgRDD = partialDepGraph.mapReduceTriplets(triplets => {
        var buffer = new ListBuffer[(Long, Double, Long)]
        val vertexData = triplets.dstAttr._2
        def dependencyMessageAccumulation(buf: ListBuffer[(Long, Double, Long)], item: (Long, Double, Long)) = {
          val messageSource = item._1
          val shortestPathList = vertexData.getPathDataMap.get(messageSource).get
          if (shortestPathList.getPredecessorPathCountMap.contains(triplets.srcId)) {
            val dependencyMessage = item
            buf += dependencyMessage
          }
          buf
        }
        // Sends your dependency to your predecessors who send you messages
        buffer = triplets.dstAttr._1.foldLeft(buffer)(dependencyMessageAccumulation)
        updateCount += buffer.size
        Iterator((triplets.srcId, buffer.toList))
      }, merge[(Long, Double, Long)]).cache()

      // Update the state of the hbseGraph
      prevG = hbseGraph
      hbseGraph = partialDepGraph.mapVertices((vid, vertexData) => vertexData._2).cache()
      // Persist them in memory
      msgRDD.count()
      hbseGraph.vertices.count()

      // Unpersist last run
      partialDepGraph.unpersistVertices(blocking = false)
      partialDepGraph.edges.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      prevMessages.unpersist(blocking = false)

    } while (!(updateCount.value == 0))
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
        runningSet.add(vertexId)
      }
      runningSet
    }
    while (pivots.size != hbseConf.pivotBatchSize && totalNumberOfPivotsUsed < vertexCount) {
      pivots ++= hbseGraph.vertices.takeSample(withReplacement = false, hbseConf.pivotBatchSize, (new Date).getTime).foldLeft(new mutable.HashSet[VertexId])(buildSetOfPivots)
      totalNumberOfPivotsUsed = previousPivots.size + pivots.size
    }
    logInfo("Pivot Selection Done")
    sc.broadcast(pivots)
  }


  def createHBSEGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexData, Long] = {
    graph.mapVertices((vid, vd) => new VertexData()).mapEdges(e => if (e.attr == 0) 1L else e.attr.toString.toLong)
  }

  def sendShortestPathRunMessage(triplet: EdgeTriplet[(mutable.HashMap[Long, ShortestPathList], VertexData), Long]) = {
    val singleMap = new mutable.HashMap[VertexId, PathData]
    val updatedPathMap = triplet.srcAttr._1
    def buildShortestPathMessage(map: mutable.HashMap[VertexId, PathData], item: (Long, ShortestPathList)) = {
      val messageSource = item._1
      val shortestPathList = item._2
      map.put(messageSource, PathData.createShortestPathMessage(messageSource, triplet.srcId, shortestPathList.getDistance + triplet.attr, shortestPathList.getShortestPathCount))
      map
    }
    logInfo(s"Sending ShortestPath Update Message to ${triplet.dstId} from ${triplet.srcId}")
    Iterator((triplet.dstId, updatedPathMap.foldLeft(singleMap)(buildShortestPathMessage)))
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
