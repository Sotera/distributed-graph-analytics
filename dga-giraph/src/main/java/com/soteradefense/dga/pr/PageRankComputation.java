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

package com.soteradefense.dga.pr;

import com.soteradefense.dga.DGALoggingUtil;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PageRankComputation extends BasicComputation<Text, DoubleWritable, Text, DoubleWritable> {

    private static final Logger logger = LoggerFactory.getLogger(PageRankComputation.class);

    public static final String MAX_EPSILON = "com.soteradefense.dga.max.epsilon";
    public static final String DAMPING_FACTOR = "damping.factor";
    public static final float DAMPING_FACTOR_DEFAULT_VALUE = 0.85f;

    @Override
    public void initialize(GraphState graphState, WorkerClientRequestProcessor<Text, DoubleWritable, Text> workerClientRequestProcessor, CentralizedServiceWorker<Text, DoubleWritable, Text> graphTaskManager, WorkerGlobalCommUsage workerGlobalCommUsage) {
        super.initialize(graphState, workerClientRequestProcessor, graphTaskManager, workerGlobalCommUsage);
        DGALoggingUtil.setDGALogLevel(this.getConf());
    }

    @Override
    public void compute(Vertex<Text, DoubleWritable, Text> vertex, Iterable<DoubleWritable> messages) throws IOException {

        float dampingFactor = this.getConf().getFloat(DAMPING_FACTOR, DAMPING_FACTOR_DEFAULT_VALUE);

        long step = getSuperstep();

        if (step == 0) {
            //set initial value
            logger.debug("Superstep is 0: Setting the default value.");
            vertex.setValue(new DoubleWritable(1.0 / getTotalNumVertices()));
        } else { // go until no one votes to continue

            double rank = 0;
            for (DoubleWritable partial : messages) {
                rank += partial.get();
            }
            rank = ((1 - dampingFactor) / getTotalNumVertices()) + (dampingFactor * rank);
            double vertexValue = vertex.getValue().get();
            double delta = Math.abs(rank - vertexValue) / vertexValue;
            aggregate(MAX_EPSILON, new DoubleWritable(delta));
            vertex.setValue(new DoubleWritable(rank));
            logger.debug("{} is calculated {} for a PageRank.", vertex.getId(), rank);
        }
        distributeRank(vertex);
    }


    private void distributeRank(Vertex<Text, DoubleWritable, Text> vertex) {
        double rank = vertex.getValue().get() / vertex.getNumEdges();
        sendMessageToAllEdges(vertex, new DoubleWritable(rank));
    }

}
