package com.soteradefense.dga.graphx.louvain

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState(var community: Long, var communitySigmaTot: Long, var internalWeight: Long, var nodeWeight: Long, var changed: Boolean) extends Serializable {

  def this() {
    this(-1L, 0L, 0L, 0L, false)
  }

  override def toString: String = {
    "{community:" + community + ",communitySigmaTot:" + communitySigmaTot +
      ",internalWeight:" + internalWeight + ",nodeWeight:" + nodeWeight + "}"
  }
}

class VertexStateSerializer extends Serializer[VertexState] {
  override def write(kryo: Kryo, output: Output, obj: VertexState): Unit = {
    kryo.writeObject(output, obj.community)
    kryo.writeObject(output, obj.communitySigmaTot)
    kryo.writeObject(output, obj.internalWeight)
    kryo.writeObject(output, obj.nodeWeight)
    kryo.writeObject(output, obj.changed)
  }

  override def read(kryo: Kryo, input: Input, classType: Class[VertexState]): VertexState = {
    new VertexState(kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]), kryo.readObject(input, classOf[Long]), kryo.readObject(input,
      classOf[Boolean]))
  }
}