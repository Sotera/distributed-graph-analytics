package com.soteradefense.dga.graphx.hbse

import scala.collection.mutable

class HighBetweennessListComparator extends Ordering[(Long, Double)] {

  override def compare(x: (Long, Double), y: (Long, Double)): Int = {
    if (x._2 < y._2) -1 else if (x._2 > y._2) 1 else 0
  }

}
//TODO: Implement Serializer
class HighBetweennessList(private var maxSize: Int, var betweennessQueue: mutable.PriorityQueue[(Long, Double)]) extends Serializable {

  def this(maxSize: Int) = this(maxSize, mutable.PriorityQueue[(Long, Double)]()(new HighBetweennessListComparator))

  def this() = this(1)

  def this(id: Long, betweenness: Double) = {
    this()
    this.betweennessQueue.enqueue((id, betweenness))
  }

  def this(maxSize: Int, id: Long, betweenness: Double) = {
    this(maxSize)
    this.betweennessQueue.enqueue((id, betweenness))
  }

  def highBetweennessSet: mutable.HashSet[Long] = {
    val set = new mutable.HashSet[Long]
    for (elem: (Long, Double) <- this.betweennessQueue)
      set.add(elem._1)
    set
  }
}
