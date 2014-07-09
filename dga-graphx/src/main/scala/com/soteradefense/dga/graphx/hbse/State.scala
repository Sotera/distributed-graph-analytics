package com.soteradefense.dga.graphx.hbse

object State extends Enumeration {
  type State = Value
  val START, PIVOT_SELECTION, SHORTEST_PATH_START, SHORTEST_PATH_RUN, PAIR_DEPENDENCY_PING_PREDECESSOR, PAIR_DEPENDENCY_FIND_SUCCESSORS, PAIR_DEPENDENCY_RUN, PAIR_DEPENDENCY_COMPLETE, FINISHED = Value
}