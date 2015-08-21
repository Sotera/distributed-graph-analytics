package com.soteradefense.dga.scan1;

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Null;
import java.io.IOException;
import java.util.HashSet;

/**
 * Created by ekimbrel on 8/20/15.
 */
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



/**
 * Scan1 determines the neighborhood size of each vertex. And reports the max value found.
 * Neighborhood size here is defined as the number of edges in the local sub-graph made up of a node and its neighboring vertices.

 * <I,V,E,M>
 * I - Vertex id
 * V - Vertex Data
 * E - Edge Data
 * M - Message type
 *
 */
public class Scan1Computation extends BasicComputation<IntWritable, IntWritable, NullWritable, ArrayPrimitiveWritable> {

    private static final Logger logger = LoggerFactory.getLogger(Scan1Computation.class);
    static final String MAX_AGG = "com.soteradefense.dga.scan1.MAX_AGG";

    @Override
    public void initialize(GraphState graphState, WorkerClientRequestProcessor<IntWritable, IntWritable, NullWritable> workerClientRequestProcessor, GraphTaskManager<IntWritable, IntWritable, NullWritable> graphTaskManager, WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
        super.initialize(graphState, workerClientRequestProcessor, graphTaskManager, workerGlobalCommUsage, workerContext);
        DGALoggingUtil.setDGALogLevel(this.getConf());
    }

    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<ArrayPrimitiveWritable> messages) throws IOException {
        try {
            if (getSuperstep() == 0) {
                broadcastNeighbors(vertex);
                return;
            }

            HashSet neighbors = new HashSet<Integer>();

            int thisNode = (int) vertex.getId().get();
            for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
                int otherNode = (int) edge.getTargetVertexId().get();
                if (thisNode != otherNode) neighbors.add(otherNode);
            }

            double neighboorhoodSize = 0.0;
            for (ArrayPrimitiveWritable incomingMessage : messages) {
                neighboorhoodSize += 1;
                int[] oneHopNeighbors = (int[]) incomingMessage.get();
                for (int node : oneHopNeighbors){
                    //because these edges will be counted twice only count them as half
                    if (neighbors.contains(node)) neighboorhoodSize+=0.5;
                }
            }

            this.aggregate(MAX_AGG, new ArrayPrimitiveWritable(new int[]{thisNode,(int) neighboorhoodSize}));
            vertex.setValue(new IntWritable( (int) neighboorhoodSize));

        } catch (Exception e) {
            System.err.print(e.toString());
        }
    }


    /**
     * Send list of neighbors to all neighbors
     *
     * @param vertex The current vertex being operated on.
     */
    private void broadcastNeighbors(Vertex<IntWritable, IntWritable, NullWritable> vertex) {

        int [] neighbors = new int[vertex.getNumEdges()];
        int i = 0;
        for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
            neighbors[i++] = edge.getTargetVertexId().get();
        }
        logger.debug("First Superstep for {}: Sending {} to all my edges.", vertex.getId(), neighbors);
        sendMessageToAllEdges(vertex,new ArrayPrimitiveWritable(neighbors));
    }


}
