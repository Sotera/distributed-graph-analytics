package com.soteradefense.dga.graphx.hbse

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

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

class PathDataSerializer extends Serializer[PathData] {
  override def write(kryo: Kryo, output: Output, obj: PathData): Unit = {
    kryo.writeObject(output, obj.getDistance)
    kryo.writeObject(output, obj.getMessageSource)
    kryo.writeObject(output, obj.getPivotSource)
    kryo.writeObject(output, obj.getDependency)
    kryo.writeObject(output, obj.getNumberOfShortestPaths)
  }

  override def read(kryo: Kryo, input: Input, classType: Class[PathData]): PathData = {
    new PathData(kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Double]), kryo.readObject(input, classOf[Long]))
  }
}