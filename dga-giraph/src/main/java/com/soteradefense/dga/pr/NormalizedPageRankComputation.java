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

import com.kenai.jffi.Array;
import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.io.IOException;


/**
 * Normalized Pagerank implementaiton to match as closely as possible the built in graphX algorithm
 */
public class NormalizedPageRankComputation extends BasicComputation<Text, PageRankData, Text, DoubleWritable> {

    private static final Logger logger = LoggerFactory.getLogger(PageRankComputation.class);

    public static final String MAX_EPSILON = "com.soteradefense.dga.max.epsilon";
    public static final String DAMPING_FACTOR = "damping.factor";
    public static final double DAMPING_FACTOR_DEFAULT_VALUE = 0.85;

    @Override
    public void initialize(GraphState graphState, WorkerClientRequestProcessor<Text, PageRankData, Text> workerClientRequestProcessor, GraphTaskManager<Text, PageRankData, Text> graphTaskManager, WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
        super.initialize(graphState, workerClientRequestProcessor, graphTaskManager, workerGlobalCommUsage, workerContext);
        DGALoggingUtil.setDGALogLevel(this.getConf());
    }

    @Override
    public void compute(Vertex<Text, PageRankData, Text> vertex, Iterable<DoubleWritable> messages) throws IOException {

        double dampingFactor = this.getConf().getDouble(DAMPING_FACTOR, DAMPING_FACTOR_DEFAULT_VALUE);

        long step = getSuperstep();

        if (step == 0) {
            // initialize the starting page rank and pagerank delta values
            vertex.setValue(new PageRankData(1.0,1.0));
        } else {

            PageRankData state = vertex.getValue();
            //if (state.delta > PageRankMasterCompute.EPSILON){
                double rank = 0;
                for (DoubleWritable partial : messages) {
                    rank += partial.get();
                }
                rank = ((1 - dampingFactor) ) + (dampingFactor * rank);
                double delta = Math.abs(rank - state.rank);
                aggregate(MAX_EPSILON, new DoubleWritable(delta));
                vertex.setValue(new PageRankData(rank,delta));
            //}

        }
        distributeRank(vertex);
    }


    private void distributeRank(Vertex<Text, PageRankData, Text> vertex) {
        double rank = vertex.getValue().rank / vertex.getNumEdges();
        sendMessageToAllEdges(vertex, new DoubleWritable(rank));
    }

}
