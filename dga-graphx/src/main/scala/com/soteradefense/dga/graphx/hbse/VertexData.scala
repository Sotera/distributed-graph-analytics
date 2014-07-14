package com.soteradefense.dga.graphx.hbse

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

import scala.collection.mutable


class VertexData(private var pathDataMap: mutable.HashMap[Long, ShortestPathList], private var partialDepMap: mutable.HashMap[Long, PartialDependency], private var approximateBetweenness: Double) extends Serializable {

  def this() = this(new mutable.HashMap[Long, ShortestPathList], new mutable.HashMap[Long, PartialDependency], 0.0)

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

class VertexDataSerializer extends Serializer[VertexData] {

  override def write(kryo: Kryo, output: Output, obj: VertexData): Unit = {
    kryo.writeObject(output, obj.getPathDataMap.size)
    obj.getPathDataMap.foreach(f => {
      kryo.writeObject(output, f._1)
      val serializer = new ShortestPathListSerializer
      serializer.write(kryo, output, f._2)
    })

    kryo.writeObject(output, obj.getPartialDependencyMap.size)
    obj.getPartialDependencyMap.foreach(f => {
      kryo.writeObject(output, f._1)
      val serializer = new PartialDependencySerializer
      serializer.write(kryo, output, f._2)
    })

    kryo.writeObject(output, obj.getApproximateBetweenness)
  }

  override def read(kryo: Kryo, input: Input, classType: Class[VertexData]): VertexData = {
    val pdSize = kryo.readObject(input, classOf[Int])
    var i = 0
    val pathDataMap = new mutable.HashMap[Long, ShortestPathList]
    for (i <- 0 to (pdSize - 1)) {
      val serializer = new ShortestPathListSerializer
      pathDataMap.put(kryo.readObject(input, classOf[Long]), serializer.read(kryo, input, classOf[ShortestPathList]))
    }
    i = 0
    val partialDepSize = kryo.readObject(input, classOf[Int])
    val partialDepMap = new mutable.HashMap[Long, PartialDependency]
    for (i <- 0 to (pdSize - 1)) {
      val serializer = new PartialDependencySerializer
      partialDepMap.put(kryo.readObject(input, classOf[Long]), serializer.read(kryo, input, classOf[PartialDependency]))
    }
    new VertexData(pathDataMap, partialDepMap, kryo.readObject(input, classOf[Double]))
  }
}