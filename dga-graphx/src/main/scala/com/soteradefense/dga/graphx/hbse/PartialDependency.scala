package com.soteradefense.dga.graphx.hbse

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

class PartialDependency(private var successors: Int, private var dependency: Double) extends Serializable {

  def this() = this(0, 0.0)

  def getDependency = this.dependency

  def getSuccessors = this.successors

  def setSuccessors(successors: Int) = this.successors = successors

  def setDependency(dep: Double) = this.dependency = dep

  def addSuccessors(diff: Int) = this.successors += diff

  def addDependency(diff: Double) = this.dependency += diff

}

class PartialDependencySerializer extends Serializer[PartialDependency] {
  override def write(kryo: Kryo, output: Output, obj: PartialDependency): Unit = {
    kryo.writeObject(output, obj.getSuccessors)
    kryo.writeObject(output, obj.getDependency)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[PartialDependency]): PartialDependency = {
    new PartialDependency(kryo.readObject(input, classOf[Int]), kryo.readObject(input, classOf[Double]))
  }
}