package com.soteradefense.dga.graphx.hbse

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

import scala.collection.mutable

class ShortestPathList(private var distance: Long, private var predecessorPathCountMap: mutable.HashMap[Long, Long]) extends Serializable {

  def this() = this(Long.MaxValue, new mutable.HashMap[Long, Long])

  def this(pathData: PathData) = {
    this()
    this.distance = pathData.getDistance
    this.predecessorPathCountMap.put(pathData.getPivotSource, pathData.getNumberOfShortestPaths)
  }

  def getPredecessorPathCountMap = this.predecessorPathCountMap

  def setPredecessorPathCountMap(map: mutable.HashMap[Long, Long]) = {
    this.predecessorPathCountMap = map
  }

  def setDistance(dist: Long) = {
    this.distance = dist
  }

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
      if (!this.predecessorPathCountMap.contains(pathData.getPivotSource)) {
        this.predecessorPathCountMap.put(pathData.getPivotSource, pathData.getNumberOfShortestPaths)
        updated = true
      } else {
        val oldNumShortestPaths = this.predecessorPathCountMap.get(pathData.getPivotSource).get
        updated = oldNumShortestPaths != pathData.getNumberOfShortestPaths
        if (updated) {
          this.predecessorPathCountMap.put(pathData.getPivotSource, pathData.getNumberOfShortestPaths)
        }
      }
    } else if (pathData.getDistance < this.distance) {
      this.distance = pathData.getDistance
      this.predecessorPathCountMap.clear()
      this.predecessorPathCountMap.put(pathData.getPivotSource, pathData.getNumberOfShortestPaths)
      updated = true
    } else {
      updated = false
    }
    updated
  }
}

class ShortestPathListSerializer extends Serializer[ShortestPathList] {
  override def write(kryo: Kryo, output: Output, obj: ShortestPathList): Unit = {
    kryo.writeObject(output, obj.getDistance)
    kryo.writeObject(output, obj.getPredecessorPathCountMap.size)
    obj.getPredecessorPathCountMap.foreach(f => {
      kryo.writeObject(output, f._1)
      kryo.writeObject(output, f._2)
    })
  }


  override def read(kryo: Kryo, input: Input, classType: Class[ShortestPathList]): ShortestPathList = {
    val distance = kryo.readObject(input, classOf[Long])
    val pathCountMapsize = kryo.readObject(input, classOf[Int])
    val map = new mutable.HashMap[Long, Long]
    var i = 0
    for (i <- 0 to pathCountMapsize) {
      map.put(kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]))
    }

    new ShortestPathList(distance, map)

  }
}