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
package com.soteradefense.dga.lc;

import com.soteradefense.dga.DGALoggingUtil;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Leaf Compression is an analytic used to compress a graph; nodes on the periphery of the graph that do not show an
 * extensive network of connections from them will inform the nodes connected to them to remove them from the graph
 * <p/>
 * This cycle continues until all leaves have been pruned.
 */
public class LeafCompressionComputation extends BasicComputation<Text, Text, Text, Text> {

    private static Logger logger = LoggerFactory.getLogger(LeafCompressionComputation.class);

    @Override
    public void initialize(GraphState graphState, WorkerClientRequestProcessor<Text, Text, Text> workerClientRequestProcessor, CentralizedServiceWorker<Text, Text, Text> graphTaskManager, WorkerGlobalCommUsage workerGlobalCommUsage) {
        super.initialize(graphState, workerClientRequestProcessor, graphTaskManager, workerGlobalCommUsage);
        DGALoggingUtil.setDGALogLevel(getConf());
    }

    @Override
    public void compute(Vertex<Text, Text, Text> vertex, Iterable<Text> messages) throws IOException {
        try {
            // Check to see if we received any messages from connected nodes notifying us
            // that they have only a single edge, and can subsequently be pruned from the graph

            for (Text incomingMessage : messages) {
                Text messageVertex = new Text(incomingMessage.toString().split(":")[0]);
                int messageValue = getVertexValue(incomingMessage.toString().split(":")[1]);
                vertex.setValue(new Text(String.format("%d", getVertexValue(vertex.getValue().toString()) + 1 + messageValue)));

                // Remove the vertex and its corresponding edge
                removeVertexRequest(messageVertex);
                vertex.removeEdges(messageVertex);
            }

            // Broadcast the edges if we only have a single edge
            sendEdges(vertex);
        } catch (Exception e) {
            System.err.println(e.toString());
        }
    }

    /**
     * Inform each node we are connected to if we only have one edge so that we can be purged from the graph, or vote
     * to halt
     *
     * @param vertex The current vertex being operated upon by the compute method
     */
    private void sendEdges(Vertex<Text, Text, Text> vertex) {
        int vertexValue = getVertexValue(vertex.getValue().toString());
        if (vertex.getNumEdges() == 1 && vertexValue != -1) {
            for (Edge<Text, Text> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), new Text(vertex.getId().toString() + ":" + vertexValue));
            }
            logger.debug("{} is being deleted.", vertex.getId());
            vertex.setValue(new Text("-1"));
            // This node will never vote to halt, but will simply be deleted.
        } else if (vertexValue != -1) {
            // If we aren't being imminently deleted
            logger.debug("{} is still in the graph.", vertex.getId());
            vertex.voteToHalt();
        }
    }

    /**
     * Converts a vertex value from a string to an int.
     *
     * @param value vertex value as a string.
     * @return integer vertex value.
     */
    private int getVertexValue(String value) {
        int vertexValue = 0;
        if (value != null && !value.equals("")) {
            vertexValue = Integer.parseInt(value);
        }
        return vertexValue;
    }
}