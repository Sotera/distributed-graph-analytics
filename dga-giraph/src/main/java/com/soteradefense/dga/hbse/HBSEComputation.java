/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.soteradefense.dga.hbse;

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;


/**
 * Calculates Shortest paths, and accumulates pair dependency information.
 * <p/>
 * Use with SBMasterCompute to find an approximated set of vertices with highest
 * betweenness centrality.
 * <p/>
 * For background information on the approximation method see:
 * See <a href="http://scholar.google.com/scholar_url?hl=en&q=http://www.researchgate.net/publication/221273491_Efficient_Extraction_of_High-Betweenness_Vertices/file/3deec52a5dd8a6faa1.pdf&sa=X&scisig=AAGBfm1Xl41dnryyDhAGnt9AYOL6iHLoOg&oi=scholarr">"W. Chong, Efficient Extraction of High-Betweenness Vertices"</a>
 * <p/>
 * For background information on the method of accumulation of pair dependencies and shortest path data see:
 * See <a href="http://www.google.com/url?url=http://scholar.google.com/scholar_url%3Fhl%3Den%26q%3Dhttp://kops.ub.uni-konstanz.de/bitstream/handle/urn:nbn:de:bsz:352-opus-71888/algorithm.pdf%253Fsequence%253D1%26sa%3DX%26scisig%3DAAGBfm2tszb3JWsE0Mp8E5os2p-udyVKtw%26oi%3Dscholarr&rct=j&q=&esrc=s&sa=X&ei=fwl1U4a9KeKgsASZqYGQAQ&ved=0CCoQgAMoADAA&usg=AFQjCNEAaZe30FmMsqHyJ1mjLJbBHcrd8w&cad=rja">"U. Brandes, A Faster Algorithm for Betweenness Centrality"</a>
 */
public class HBSEComputation extends AbstractComputation<Text, VertexData, Text, PathData, PathData> {

    private static final Logger logger = LoggerFactory.getLogger(HBSEComputation.class);

    @Override
    public void initialize(GraphState graphState, WorkerClientRequestProcessor<Text, VertexData, Text> workerClientRequestProcessor, GraphTaskManager<Text, VertexData, Text> graphTaskManager, WorkerAggregatorUsage workerAggregatorUsage, WorkerContext workerContext) {
        super.initialize(graphState, workerClientRequestProcessor, graphTaskManager, workerAggregatorUsage, workerContext);
        DGALoggingUtil.setDGALogLevel(this.getConf());
    }

    /**
     * Works in two major stages which are repeated and coordinated
     * by the setting of a global state (set by the master compute class)
     * <p/>
     * <p/>
     * <ul>
     * <li>Stage 1:  Discover shortest paths, and shortest path counts for each source vertex in a globally set pivot batch to each node in the graph.</li>
     * <li>Stage 2:  Accumulate pair dependencies.</li>
     * </ul>
     */
    @Override
    public void compute(Vertex<Text, VertexData, Text> vertex, Iterable<PathData> messages) throws IOException {
        long step = this.getSuperstep();
        String id = vertex.getId().toString();
        int updateCount = 0;
        State state = getCurrentGlobalState();

        if (step == 0) {
            vertex.setValue(new VertexData());
        }

        VertexData vertexValue = vertex.getValue();

        // Start a shortest path phase
        // if this vertex is a source (pivot) send shortest path messages to neighbors

        if (State.SHORTEST_PATH_START == state) {
            logger.info("Started the Pivot Selection Process.");
            if (!vertexValue.getWasPivotPoint() && isPivotPoint(id)) {
                vertexValue.setWasPivotPoint(true);
                aggregate(HBSEMasterCompute.PIVOT_AGG, new IntWritable(1));
                logger.info("Superstep: " + step + " Start new shortest path computation. Source = " + id);
                for (Edge<Text, Text> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), PathData.getShortestPathMessage(id, id, getEdgeValue(edge.getValue().toString()), 1));
                }
                vertexValue.addPathData(PathData.getShortestPathMessage(id, id, 0, 1L));
                this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));
            }

        }


        // continue a shortest path phase, continues until no shortest paths are updated.
        if (State.SHORTEST_PATH_RUN == state) {

            Map<String, ShortestPathList> updatedPathMap = new HashMap<String, ShortestPathList>();

            // process incoming messages
            for (PathData message : messages) {
                ShortestPathList updatedPath = vertexValue.addPathData(message);
                if (updatedPath != null) {
                    updatedPathMap.put(message.getSource(), updatedPath);
                }
            }

            // send outgoing messages for each updated shortest path
            for (Entry<String, ShortestPathList> entry : updatedPathMap.entrySet()) {
                ShortestPathList spl = entry.getValue();
                String src = entry.getKey();
                long numPaths = spl.getShortestPathCount();
                updateCount++;
                for (Edge<Text, Text> edge : vertex.getEdges()) {
                    long newDistance = spl.getDistance() + getEdgeValue(edge.getValue().toString());
                    this.sendMessage(edge.getTargetVertexId(), PathData.getShortestPathMessage(src, id, newDistance, numPaths));
                }
            }

            this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(updateCount));
        }


        // Start of the dependency accumulation phase, each vertex must determine if it has any successors
        if (State.PAIR_DEPENDENCY_PING_PREDECESSOR == state) {
            StringBuilder builder = new StringBuilder();
            // for each shortest path send a message with that source.
            for (Entry<String, ShortestPathList> entry : vertexValue.getPathDataMap().entrySet()) {
                String source = entry.getKey();
                long distance = entry.getValue().getDistance();
                if (distance > 0) { // exclude this vertex
                    for (String pred : entry.getValue().getPredecessorPathCountMap().keySet()) {
                        this.sendMessage(new Text(pred), PathData.getPingMessage(source));
                        builder.append("(").append(pred).append(",").append(source).append("),");
                    }
                }

            }
            if (builder.length() > 1) builder.deleteCharAt(builder.length() - 1);
            logger.trace("ID: " + id + " Step: " + step + " State: " + state + " sent messages (pred,source):  " + builder.toString());
        }


        // process ping messages form PAIR_DEPENDENCY_PING_PREDECESSOR to determine if
        // this vertex has any successors
        // vertices with no successors will begin the pair dependency accumulation process.
        if (State.PAIR_DEPENDENCY_FIND_SUCCESSORS == state) {
            Set<String> successorExists = new HashSet<String>();

            for (PathData message : messages) {
                String src = message.getSource();
                successorExists.add(src);
                vertexValue.addPartialDep(src, 0.0, 1);  // for every successor message ad one to the partial dep count
            }

            StringBuilder builder = new StringBuilder();
            Set<String> allPaths = vertexValue.getPathDataMap().keySet();
            allPaths.remove(vertex.getId().toString());
            Set<String> noSuccessor = new HashSet<String>();
            for (String src : allPaths) {
                if (!successorExists.contains(src)) {
                    noSuccessor.add(src);
                    builder.append(src).append(",");
                }
            }

            // for any sources that this vertex has no successors, start the dependency accumulation chain
            if (noSuccessor.size() > 0) {
                for (String src : noSuccessor) {
                    ShortestPathList spl = vertexValue.getPathDataMap().get(src);
                    long numPaths = spl.getShortestPathCount();
                    double dep = 0;
                    for (String predecessor : spl.getPredecessorPathCountMap().keySet()) {
                        this.sendMessage(new Text(predecessor), PathData.getDependencyMessage(src, dep, numPaths));
                    }
                }
                noSuccessor.clear();
            }

            if (builder.length() > 1) builder.deleteCharAt(builder.length() - 1);
            logger.trace("ID: " + id + " Step: " + step + " State: " + state + " set noSuccessor " + builder.toString());

        }


        // continue the pair dependency phase until no updates are done / all dependencies are accumulated.
        if (State.PAIR_DEPENDENCY_RUN == state) {

            for (PathData message : messages) {
                String src = message.getSource();

                if (src.equals(id)) {
                    continue; // don't process paths for which you are the source
                }

                double successorDep = message.getDependency();
                long successorNumPaths = message.getNumPaths();
                long numPaths = vertexValue.getPathDataMap().get(src).getShortestPathCount();
                double partialDep = ((double) numPaths / successorNumPaths) * (1 + successorDep);
                logger.debug("ID: " + id + " Step: " + step + " message {src:" + src + " successorPaths:" + successorNumPaths + " successorDep:" + successorDep + "} calculated {paths:" + numPaths + ", dep:" + partialDep + "}");

                // accumulate the dependency and subtract one successor
                PartialDependency partialSum = vertexValue.addPartialDep(src, partialDep, -1);

                // if the successor count reaches 0 pass this vertex dependency on to predecessors
                if (partialSum.getSuccessors() == 0) {
                    ShortestPathList spl = vertexValue.getPathDataMap().get(src);
                    StringBuilder builder = new StringBuilder();
                    this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));
                    for (String predecessor : spl.getPredecessorPathCountMap().keySet()) {
                        this.sendMessage(new Text(predecessor), PathData.getDependencyMessage(src, partialSum.getDependency(), numPaths));
                        builder.append(predecessor).append(",");
                    }
                    if (builder.length() > 1) builder.deleteCharAt(builder.length() - 1);
                    logger.debug("ID: " + id + " Step: " + step + " forwarding partial dep to predecessors (" + builder.toString() + ") {src:" + src + ", paths:" + numPaths + ", dep:" + partialSum.getDependency() + "}");
                }
            }
        }


        // completed dependency accumulation. calculate current betweenness value and clear all other vertex data
        // to prepare for next stage (which will be SHORTEST_PATH_START or the computation will halt, based on high betweenness
        // set stability, as determined in the master compute class.
        if (State.PAIR_DEPENDENCY_COMPLETE == state) {
            double approxBetweenness = vertexValue.getApproxBetweenness();
            for (PartialDependency partialDependency : vertexValue.getPartialDependencyMap().values()) {
                approxBetweenness += partialDependency.getDependency();
            }
            vertexValue.setApproxBetweenness(approxBetweenness);
            vertexValue.getPartialDependencyMap().clear();
            vertexValue.getPathDataMap().clear();
            this.aggregate(HBSEMasterCompute.HIGH_BC_SET_AGG, getNewHighBetweennessList(id, approxBetweenness));
        }


    }

    /**
     * Parses the Edge Value from a string.
     * @param s Edge value as a string
     * @return long edge value
     */
    private long getEdgeValue(String s){
        long edgeValue;
        try{
            edgeValue = Long.parseLong(s);
        }
        catch(NumberFormatException ex){
            edgeValue = 1;
        }
        return edgeValue;
    }

    /**
     * Return the current global state
     *
     * @return State that stores the current global state
     */
    private State getCurrentGlobalState() {
        IntWritable stateInt = this.getAggregatedValue(HBSEMasterCompute.STATE_AGG);
        return State.values()[stateInt.get()];
    }


    /**
     * Get a new HighBetweennessList object, configured with the betweenness.set.maxSize option
     * from the job conf. If not set size will default to 1.
     *
     * @param id    Vertex Id
     * @param value Betweenness Value
     * @return an empty HighBetweennessList object.
     */
    public HighBetweennessList getNewHighBetweennessList(String id, double value) {
        int size;
        try {
            size = Integer.parseInt(getConf().get(HBSEMasterCompute.BETWEENNESS_SET_MAX_SIZE));
        } catch (NumberFormatException e) {
            logger.error("betweenness.set.maxSize must be set to a valid int.");
            throw e;
        }
        return new HighBetweennessList(size, id, value);
    }

    /**
     * Returns the percentage of nodes to count as a pivot.
     *
     * @return Pivot based on the superstep.
     */
    private double getPercentageCutOff() {
        if (getSuperstep() == 0) {
            return ((DoubleWritable) getAggregatedValue(HBSEMasterCompute.INITIAL_PIVOT_PERCENT)).get();
        } else {
            return ((DoubleWritable) getAggregatedValue(HBSEMasterCompute.PIVOT_PERCENT)).get();
        }
    }

    /**
     * Determines if a vertex can be a pivot point.
     *
     * @param id Vertex Id
     * @return True if it is a pivot point.
     */
    private boolean isPivotPoint(String id) {
        Random random = getRandomWithSeed(HBSEMasterCompute.PIVOT_BATCH_RANDOM_SEED);
        double percentageCutoff = getPercentageCutOff();
        double randomNumber = random.nextDouble();
        boolean isPivot = randomNumber < percentageCutoff;
        logger.info("Selected as a possible pivot: " + id);
        return isPivot;
    }


    /**
     * Gets a Random Object With A possible Seed based on a Configuration.
     *
     * @param name Configuration Key
     * @return Random which is either seeded or not.
     */
    private Random getRandomWithSeed(String name) {
        Random random;
        String randomSeedStr = getConf().get(name);
        if (randomSeedStr == null) {
            random = new Random();
        } else {
            long seed = Long.parseLong(randomSeedStr);
            random = new Random(seed);
            logger.info("Set random seed: " + seed);
        }
        return random;
    }
}
