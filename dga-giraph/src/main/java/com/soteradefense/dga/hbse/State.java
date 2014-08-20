/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

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
