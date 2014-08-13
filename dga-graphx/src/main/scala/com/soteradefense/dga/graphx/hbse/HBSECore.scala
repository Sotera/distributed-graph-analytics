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
    var currentBetweennessMap: mutable.Set[(Long, Double)] = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var runningBetweennessMap: mutable.Set[(Long, Double)] = new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet)
    var delta: Int = 0
    var keepRunning: Boolean = true
    var stabilityCutOffMetCount: Int = 0
    var hbseGraph: Graph[VertexData, Long] = createHBSEGraph(graph).cache()
    // So we dont have to recalculate every run.
    val totalNumberOfVertices = hbseGraph.vertices.count()
    do {
      stabilityCutOffMetCount = 0
      logInfo("Selecting Pivots")
      val pivots: Broadcast[mutable.Set[VertexId]] = selectPivots(sc, hbseGraph, totalNumberOfVertices)
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
      val shouldKeepRunningResult: (Boolean, Int) = shouldKeepRunning(delta, stabilityCutOffMetCount, numberOfPivotsSelected, totalNumberOfVertices)
      keepRunning = shouldKeepRunningResult._1
      stabilityCutOffMetCount = shouldKeepRunningResult._2

    } while (keepRunning)
    // Create an RDD to write the High Betweenness Set.
    val betweennessVertices = sc.parallelize(runningBetweennessMap.toSeq)
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

  def getHighBetweennessSet(graph: Graph[VertexData, Long], runningBetweennessSet: mutable.Set[(Long, Double)]) = {
    (runningBetweennessSet ++ graph.vertices.map(f => (f._1,
      f._2.getApproximateBetweenness)).takeOrdered(hbseConf.betweennessSetMaxSize)
      (orderedBetweennessSet))
      .groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum)).foldLeft(new mutable.TreeSet[(Long, Double)]()(orderedBetweennessSet))((b, a) => b += a)
  }

  def compareHighBetweennessSets(x: mutable.Set[(Long, Double)], y: mutable.Set[(Long, Double)]) = {
    y.map(f => f._1).diff(x.map(g => g._1)).size
  }

  def computeHighBetweenness(graph: Graph[VertexData, Long]) = {
    val hbseGraph = graph.cache()

    val betweennessGraph = hbseGraph.mapVertices((vertexId, vertexData) => {
      def computeBetweenness(total: Double, item: (Long, PartialDependency)) = {
        val partialDependency = item._2
        total + partialDependency.getDependency
      }
      var approxBetweenness = vertexData.getApproximateBetweenness
      approxBetweenness = vertexData.getPartialDependencyMap.foldLeft(0.0)(computeBetweenness)
      vertexData.setApproxBetweenness(approxBetweenness)
      vertexData.getPartialDependencyMap.clear()
      vertexData.getPathDataMap.clear()
      vertexData
    }).cache()
    betweennessGraph.vertices.count()
    //    hbseGraph.unpersistVertices(blocking = false)
    //    hbseGraph.edges.unpersist(blocking = false)
    betweennessGraph
  }

  def shortestPathRun(graph: Graph[VertexData, Long], pivots: Broadcast[mutable.Set[VertexId]], sc: SparkContext) = {
    var shortestPathPhasesCompleted = 0
    val hbseGraph = graph.cache()
    var messageRDD: VertexRDD[mutable.Map[Long, PathData]] = null
    do {
      val validEdges = hbseGraph.edges.filter(edgePredicate => pivots.value.contains(edgePredicate.srcId))
      val newGraph = Graph.fromEdges(validEdges, None)
      messageRDD = newGraph.mapReduceTriplets(triplet => {
        // Add a PathData to my node.
        val singleMessageMap = new mutable.HashMap[VertexId, PathData]
        // Send a Shortest Path Message to my neighbor.
        singleMessageMap.put(triplet.srcId, PathData.createShortestPathMessage(triplet.srcId, triplet.srcId, triplet.attr, 1))
        logInfo(s"Sending ShortestPath Message to ${triplet.dstId} from ${triplet.srcId}")
        // Needs to be a list because all PathData messages need to be sent to the node.
        Iterator((triplet.dstId, singleMessageMap))
      }, mergeMapMessage).cache()

      messageRDD.count()

      newGraph.unpersistVertices(blocking = false)
      newGraph.edges.unpersist(blocking = false)
      var updateCount: Accumulator[Int] = null

      // Shortest Path Run
      do {
        updateCount = sc.accumulator(0)(SparkContext.IntAccumulatorParam)
        // Join the HBSEGraph with the VertexRDD to Process the Messages
        //TODO: Removing CACHE
        val updatedPaths = hbseGraph.outerJoinVertices(messageRDD)((vertexId, vertexData, shortestPathMessages) => {
          //Stores the Paths that were updated
          val fullUpdatedPathMap = new mutable.HashMap[Long, ShortestPathList]
          //Process Incoming Messages
          if (pivots.value.contains(vertexId) && !vertexData.getPathDataMap.contains(vertexId))
            vertexData.addPathData(PathData.createShortestPathMessage(vertexId, vertexId, 0, 1L))

          def pathUpdateAccumulation(updatedPathMap: mutable.HashMap[Long, ShortestPathList], item: (Long, PathData)) = {
            //Add the PathData to the current vertex
            val pd = item._2
            val updatedPath = vertexData.addPathData(pd)
            if (updatedPath != null) {
              //If it updated, add it to the updatedPathMap
              logInfo(s"Path Updated ${pd.getMessageSource} for Vertex: $vertexId")
              updatedPathMap.put(pd.getMessageSource, updatedPath)
              updateCount += 1
            }
            updatedPathMap
          }
          shortestPathMessages.getOrElse(Map.empty).foldLeft(fullUpdatedPathMap)(pathUpdateAccumulation)
        })

        //Needed to Persist the update count for some reason
        //Get the update count based on the size of each hashmap
        logInfo(s"Update Count is: $updateCount")
        //Forward the updated paths to the next edge
        val prevMessages = messageRDD
        messageRDD = updatedPaths.mapReduceTriplets(sendShortestPathRunMessage, mergeMapMessage).cache()
        messageRDD.count()

        //        updatedPaths.unpersistVertices(blocking = false)
        //        updatedPaths.edges.unpersist(blocking = false)
        prevMessages.unpersist(blocking = false)


      } while (!(updateCount.value == 0))
      // Increase the Number of Completed Phases
      shortestPathPhasesCompleted += 1
    } while (!(shortestPathPhasesCompleted == hbseConf.shortestPathPhases))

    hbseGraph
  }

  def pingPredecessorsAndFindSuccessors(graph: Graph[VertexData, Long], sc: SparkContext) = {
    var hbseGraph = graph.cache()
    //Ping Predecessor
    val pingRDD = hbseGraph.mapReduceTriplets(sendPingMessage, merge[Long]).cache()

    logInfo("Processing Nodes with No Successors")
    //TODO: Removing Cache
    val mergedGraph = hbseGraph.outerJoinVertices(pingRDD)((vertexId, vertexData, pingMessages) => {
      val validMessages = pingMessages.getOrElse(List.empty)
      validMessages.foreach(source => {
        logInfo(s"Adding Partial Dependency for: $source")
        vertexData.addPartialDependency(source, 0.0, 1)
      })
      def noSuccessorAccumulation(set: mutable.HashSet[Long], item: (Long, ShortestPathList)) = {
        val vertexIdInMyPath = item._1
        if (vertexIdInMyPath != vertexId && !validMessages.contains(vertexIdInMyPath))
          set.add(vertexIdInMyPath)
        set
      }
      val noSuccessor = vertexData.getPathDataMap.foldLeft(new mutable.HashSet[Long])(noSuccessorAccumulation)
      logInfo(s"No Successor Count is: ${noSuccessor.size}")
      (noSuccessor, vertexData)
    }).cache()

    //mergedGraph.vertices.count()
    logInfo("Sending Dependency Messages")
    // Find Successors
    var msgRDD = mergedGraph.mapReduceTriplets(sendDependencyMessage, merge[(Long, Double, Long)]).cache()
    hbseGraph = mergedGraph.mapVertices((vertexId, vertexData) => {
      val originalVertexData = vertexData._2
      originalVertexData
    }).cache()
    hbseGraph.vertices.count()

    //Collects the values
    msgRDD.count()


    var updateCount: Accumulator[Int] = null

    pingRDD.unpersist(blocking = false)
    // Pair Dependency Run State
    do {
      updateCount = sc.accumulator(0)(SparkContext.IntAccumulatorParam)
      //TODO: Removing Cache
      val partialDepGraph = hbseGraph.outerJoinVertices(msgRDD)((vertexId, vertexData, predecessorList) => {
        var newBuf: ListBuffer[(Boolean, (Long, Double, Long), ShortestPathList)] = new ListBuffer[(Boolean, (Long, Double, Long), ShortestPathList)]
        def partialDependencyCalculation(accumulatedDependencies: ListBuffer[(Boolean, (Long, Double, Long), ShortestPathList)], partialDependency: (Long, Double, Long)) = {
          val messageSource = partialDependency._1
          if (messageSource != vertexId) {
            val successorDep = partialDependency._2
            val successorNumberOfPaths = partialDependency._3
            val numPaths = vertexData.getPathDataMap.get(messageSource).get.getShortestPathCount
            val partialDep = (numPaths.toDouble / successorNumberOfPaths.toDouble) * (1 + successorDep)
            val partialSum = vertexData.addPartialDependency(messageSource, partialDep, -1)
            val listItem = (partialSum.getSuccessors == 0, Tuple3(messageSource, partialSum.getDependency, numPaths), vertexData.getPathDataMap.get(messageSource).get)
            accumulatedDependencies += listItem
          }
          accumulatedDependencies
        }
        newBuf = predecessorList.getOrElse(List.empty).foldLeft(newBuf)(partialDependencyCalculation)
        newBuf.toList
      })

      val prevMessages = msgRDD
      msgRDD = partialDepGraph.mapReduceTriplets(triplets => {
        var buffer = new ListBuffer[(Long, Double, Long)]
        def dependencyMessageAccumulation(buf: ListBuffer[(Long, Double, Long)], item: (Boolean, (Long, Double, Long), ShortestPathList)) = {
          val shortestPathList = item._3
          val successorsIsZero = item._1
          if (successorsIsZero && shortestPathList.getPredecessorPathCountMap.contains(triplets.srcId)) {
            val dependencyMessage = item._2
            buf += dependencyMessage
          }
          buf
        }
        buffer = triplets.dstAttr.foldLeft(buffer)(dependencyMessageAccumulation)
        updateCount += buffer.size
        Iterator((triplets.srcId, buffer.toList))
      }, merge[(Long, Double, Long)]).cache()

      //Collects the values
      msgRDD.count()

      //      partialDepGraph.unpersistVertices(blocking = false)
      //      partialDepGraph.edges.unpersist(blocking = false)
      prevMessages.unpersist(blocking = false)
    } while (!(updateCount.value == 0))
    mergedGraph.unpersistVertices(blocking = false)
    mergedGraph.edges.unpersist(blocking = false)
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
    def op(buf: ListBuffer[Long], item: (Long, ShortestPathList)) = {
      val shortestPathList = item._2
      val pingVertexId = item._1
      if (shortestPathList.getDistance > 0 && shortestPathList.getPredecessorPathCountMap.contains(triplet.srcId)) {
        buf += pingVertexId
      }
      buf
    }
    buffer = triplet.dstAttr.getPathDataMap.foldLeft(buffer)(op)
    Iterator((triplet.srcId, buffer.toList))
  }

  def selectPivots(sc: SparkContext, hbseGraph: Graph[VertexData, Long], vertexCount: Long) = {
    var totalNumberOfPivotsUsed = previousPivots.size
    var pivots: mutable.Set[VertexId] = new mutable.HashSet[VertexId]
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
    val singleMap = new mutable.HashMap[VertexId, PathData]
    val updatedPathMap = triplet.srcAttr
    def op(map: mutable.HashMap[VertexId, PathData], item: (Long, ShortestPathList)) = {
      val messageSource = item._1
      val shortestPathList = item._2
      map.put(messageSource, PathData.createShortestPathMessage(messageSource, triplet.srcId, shortestPathList.getDistance + triplet.attr, shortestPathList.getShortestPathCount))
      map
    }
    logInfo(s"Sending ShortestPath Update Message to ${triplet.dstId} from ${triplet.srcId}")
    Iterator((triplet.dstId, updatedPathMap.foldLeft(singleMap)(op)))
  }

  def merge[T: ClassTag](leftList: List[T], rightList: List[T]) = {
    leftList ++ rightList
  }

  def mergeMapMessage(leftMessageMap: mutable.Map[Long, PathData], rightMessageMap: mutable.Map[Long, PathData]) = {
    def op(mergedMessageMap: mutable.Map[Long, PathData], item: (Long, PathData)) = {
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
    leftMessageMap.foldLeft(rightMessageMap)(op)
  }
}
