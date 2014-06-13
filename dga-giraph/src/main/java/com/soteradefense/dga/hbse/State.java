package com.soteradefense.dga.hbse;

/**
 * Global States that direct certain computation.
 * <ul>
 * <li>START: Chooses the initial batch size.</li>
 * <li>SHORTEST_PATH_START: Starts to calculate the shortest paths from the pivots to every other node.</li>
 * <li>SHORTEST_PATH_RUN: Instructs the nodes to actually find the shortest paths.</li>
 * <li>PAIR_DEPENDENCY_PING_PREDECESSOR:  Sends a message to all Predecessors, letting them know they are dependent on them being in the graph.</li>
 * <li>PAIR_DEPENDENCY_FIND_SUCCESSORS:  Process all the messages you receive from nodes that are ahead of the current vertex. For any source that has no successors, it will begin to pair itself with nodes that it depends on.</li>
 * <li>PAIR_DEPENDENCY_RUN: Will continue to accumulate all dependencies until all dependencies are accounted for.</li>
 * <li>PAIR_DEPENDENCY_COMPLETE:  Go through the nodes that are dependencies and calculate the approx. betweenness value.</li>
 * <li>FINISHED:  Computation is Halted.</li>
 * </ul>
 */
public enum State {
    START,
    PIVOT_SELECTION,
    SHORTEST_PATH_START,
    SHORTEST_PATH_RUN,
    PAIR_DEPENDENCY_PING_PREDECESSOR,
    PAIR_DEPENDENCY_FIND_SUCCESSORS,
    PAIR_DEPENDENCY_RUN,
    PAIR_DEPENDENCY_COMPLETE,
    FINISHED
}
