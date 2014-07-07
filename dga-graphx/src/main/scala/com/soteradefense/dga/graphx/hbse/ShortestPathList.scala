package com.soteradefense.dga.graphx.hbse

import scala.collection.mutable

class ShortestPathList(private var distance: Long, private var predecessorPathCountMap: mutable.HashMap[Long, Long]) extends Serializable {

  def this() = this(Long.MaxValue, new mutable.HashMap[Long, Long])

  def this(pathData: PathData) = {
    this()
    this.distance = pathData.getDistance
    this.predecessorPathCountMap.put(pathData.getDestination, pathData.getNumberOfShortestPaths)
  }

  def getPredecessorPathCountMap = this.predecessorPathCountMap

  def getDistance = this.distance

  def getShortestPathCount = {
    var paths: Long = 0L
    for (dist: Long <- this.predecessorPathCountMap.values)
      paths += dist
    paths
  }

  def update(pathData: PathData): Boolean = {
    var updated: Boolean = false
    if (this.distance == pathData.getDistance) {
      if (!this.predecessorPathCountMap.contains(pathData.getDestination)) {
        this.predecessorPathCountMap.put(pathData.getDestination, pathData.getNumberOfShortestPaths)
        updated = true
      } else {
        val oldNumShortestPaths = this.predecessorPathCountMap.get(pathData.getDestination).get
        updated = oldNumShortestPaths != pathData.getNumberOfShortestPaths
        if (updated) {
          this.predecessorPathCountMap.put(pathData.getDestination, pathData.getNumberOfShortestPaths)
        }
      }
    } else if (pathData.getDistance < this.distance) {
      this.distance = pathData.getDistance
      this.predecessorPathCountMap.clear()
      this.predecessorPathCountMap.put(pathData.getDestination, pathData.getNumberOfShortestPaths)
      updated = true
    } else {
      updated = false
    }
    updated
  }
}
