package com.soteradefense.dga.graphx.hbse

import scala.collection.mutable


class VertexData(private var pathDataMap: mutable.HashMap[Long, ShortestPathList], private var partialDepMap: mutable.HashMap[Long, PartialDependency], var wasPivotPoint: Boolean,
                 private var approximateBetweenness: Double) extends Serializable {

  def this() = this(new mutable.HashMap[Long, ShortestPathList], new mutable.HashMap[Long, PartialDependency], false, 0.0)

  def addPathData(pathData: PathData): ShortestPathList = {
    var list: ShortestPathList = null
    val source: Long = pathData.getMessageSource
    if (!pathDataMap.contains(source)) {
      list = new ShortestPathList(pathData)
      this.pathDataMap.put(source, list)
    }
    else {
      list = pathDataMap.get(source).get
      list = if (list.update(pathData)) list else null
    }
    list
  }

  def getApproximateBetweenness = this.approximateBetweenness

  def setApproxBetweenness(approx: Double) {
    this.approximateBetweenness = approx
  }

  def getPathDataMap = this.pathDataMap

  def addPartialDependency(src: Long, dependency: Double, numberOfSuccessors: Int): PartialDependency = {
    var current: PartialDependency = null
    if (this.partialDepMap.contains(src)) {
      current = this.partialDepMap.get(src).get
      current.addDependency(dependency)
      current.addSuccessors(numberOfSuccessors)
    }
    else {
      current = new PartialDependency(numberOfSuccessors, dependency)
      this.partialDepMap.put(src, current)
    }
    current

  }

  def getPartialDependencyMap = this.partialDepMap

}
