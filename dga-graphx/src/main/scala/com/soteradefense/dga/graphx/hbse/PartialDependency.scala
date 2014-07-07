package com.soteradefense.dga.graphx.hbse

//TODO: Write Serializer
class PartialDependency(private var successors: Int, private var dependency: Double) extends Serializable {
  def this() = this(0, 0.0)

  def getDependency = this.dependency

  def getSuccessors = this.successors

  def addSuccessors(diff: Int) = this.successors += diff

  def addDependency(diff: Double) = this.dependency += diff

}
