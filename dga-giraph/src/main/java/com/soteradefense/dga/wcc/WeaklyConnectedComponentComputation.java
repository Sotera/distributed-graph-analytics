/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.soteradefense.dga.wcc;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * WeaklyConnectedComponents is the concept of finding how many uniquely connected nodes there are in a specific data set.
 */
public class WeaklyConnectedComponentComputation extends BasicComputation<Text, Text, Text, Text> {

    @Override
    public void compute(Vertex<Text, Text, Text> vertex, Iterable<Text> messages) throws IOException {
        try {
            if (getSuperstep() == 0) {
                broadcastGreatestNeighbor(vertex);
                return;
            }
            boolean changed = false;
            String maxId = vertex.getValue().toString();
            for (Text incomingMessage : messages) {
                if (maxId.compareTo(incomingMessage.toString()) < 0) {
                    maxId = incomingMessage.toString();
                    changed = true;
                }
            }
            broadcastUpdates(vertex, changed, maxId);
        } catch (Exception e) {
            System.err.print(e.toString());
        }
    }

    /**
     * Only called during the first superstep.
     * For Each Edge, find the one who has the greatest id and broadcast that to all neighbors.
     *
     * @param vertex The current vertex being operated on.
     */
    private void broadcastGreatestNeighbor(Vertex<Text, Text, Text> vertex) {
        String maxId = vertex.getId().toString();
        for (Edge<Text, Text> edge : vertex.getEdges()) {
            if (maxId.compareTo(edge.getTargetVertexId().toString()) < 0) {
                maxId = edge.getTargetVertexId().toString();
            }
        }
        broadcastUpdates(vertex, true, maxId);
    }

    /**
     * Sends a message to all neighbors if the greatest value has changed.
     *
     * @param vertex  The current vertex.
     * @param changed Has the greatest value changed?
     * @param maxId   The current id that has the greatest value.
     */
    private void broadcastUpdates(Vertex<Text, Text, Text> vertex, boolean changed, String maxId) {
        if (changed) {
            vertex.setValue(new Text(maxId));
            sendMessageToAllEdges(vertex, new Text(vertex.getValue().toString()));
        }
        vertex.voteToHalt();
    }
}
