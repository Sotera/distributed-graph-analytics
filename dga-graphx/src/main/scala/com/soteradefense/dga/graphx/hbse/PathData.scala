package com.soteradefense.dga.graphx.hbse

//TODO: Implement Serialization
class PathData(private var distance: Long, private var messageSource: Long, private var pivotSource: Long, private var dependency: Double, private var numPaths: Long) extends Serializable {
  def this() = this(Long.MaxValue, -1, -1, -1, -1)

  def getDistance = this.distance

  def getMessageSource = this.messageSource

  def getPivotSource = this.pivotSource

  def getDependency = this.dependency

  def getNumberOfShortestPaths = this.numPaths
}

object PathData {
  def createShortestPathMessage(src: Long, dst: Long, distance: Long, numPaths: Long) = new PathData(distance, src, dst, -1, numPaths)

  def createPingMessage(src: Long) = new PathData(-1, src, -1, -1, -1)

  def createDependencyMessage(src: Long, dep: Double, numPaths: Long) = new PathData(-1, src, -1, dep, numPaths)
}