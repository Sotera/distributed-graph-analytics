package com.soteradefense.dga.graphx.louvain

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