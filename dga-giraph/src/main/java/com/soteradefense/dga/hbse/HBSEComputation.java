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

import com.soteradefense.dga.DGALoggingUtil;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
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
    public void initialize(GraphState graphState, WorkerClientRequestProcessor<Text, VertexData, Text> workerClientRequestProcessor, CentralizedServiceWorker<Text, VertexData, Text> graphTaskManager, WorkerGlobalCommUsage workerGlobalCommUsage) {
        super.initialize(graphState, workerClientRequestProcessor, graphTaskManager, workerGlobalCommUsage);
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
        StringBuilder builder;
        long step = this.getSuperstep();
        String id = vertex.getId().toString();
        int updateCount = 0;
        State state = getCurrentGlobalState();

        if (step == 0) {
            vertex.setValue(new VertexData());
        }

        VertexData vertexValue = vertex.getValue();
        switch (state) {
            case PIVOT_SELECTION:
                logger.debug("Started the Pivot Selection Process.");
                if (!vertexValue.getWasPivotPoint() && isPossiblePivotPoint(id)) {
                    logger.debug("{} has been selected.", vertex.getId());
                    aggregate(HBSEMasterCompute.PIVOT_AGG, new PivotList(id));
                }
                break;
            // Start a shortest path phase
            // if this vertex is a source (pivot) send shortest path messages to neighbors
            case SHORTEST_PATH_START:
                if (isPivotPoint(id)) {
                    vertexValue.setWasPivotPoint(true);
                    logger.debug("Superstep: {} Start new shortest path computation. Source = {}", step, id);
                    for (Edge<Text, Text> edge : vertex.getEdges()) {
                        sendMessage(edge.getTargetVertexId(), PathData.getShortestPathMessage(id, id, getEdgeValue(edge.getValue().toString()), BigInteger.valueOf(1)));
                    }
                    vertexValue.addPathData(PathData.getShortestPathMessage(id, id, BigInteger.valueOf(0), BigInteger.valueOf(1L)));
                    this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));
                }
                break;
            // continue a shortest path phase, continues until no shortest paths are updated.
            case SHORTEST_PATH_RUN:
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
                    BigInteger numPaths = spl.getShortestPathCount();
                    updateCount++;
                    for (Edge<Text, Text> edge : vertex.getEdges()) {
                        BigInteger newDistance = spl.getDistance().add(getEdgeValue(edge.getValue().toString()));
                        this.sendMessage(edge.getTargetVertexId(), PathData.getShortestPathMessage(src, id, newDistance, numPaths));
                    }
                }

                this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(updateCount));
                break;
            // Start of the dependency accumulation phase, each vertex must determine if it has any successors
            case PAIR_DEPENDENCY_PING_PREDECESSOR:
                builder = new StringBuilder();
                // for each shortest path send a message with that source.
                for (Entry<String, ShortestPathList> entry : vertexValue.getPathDataMap().entrySet()) {
                    String source = entry.getKey();
                    BigInteger distance = entry.getValue().getDistance();
                    if (distance.compareTo(BigInteger.ZERO) > 0) { // exclude this vertex
                        for (String pred : entry.getValue().getPredecessorPathCountMap().keySet()) {
                            this.sendMessage(new Text(pred), PathData.getPingMessage(source));
                            builder.append("(").append(pred).append(",").append(source).append("),");
                        }
                    }

                }
                if (builder.length() > 1) builder.deleteCharAt(builder.length() - 1);
                logger.trace("ID: {} Step: {} State: {} send messages (pred, source): {}", id, step, state, builder.toString());
                break;
            // process ping messages form PAIR_DEPENDENCY_PING_PREDECESSOR to determine if
            // this vertex has any successors
            // vertices with no successors will begin the pair dependency accumulation process.
            case PAIR_DEPENDENCY_FIND_SUCCESSORS:
                Set<String> successorExists = new HashSet<String>();

                for (PathData message : messages) {
                    String src = message.getSource();
                    successorExists.add(src);
                    vertexValue.addPartialDep(src, 0.0, 1);  // for every successor message ad one to the partial dep count
                }

                builder = new StringBuilder();
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
                        BigInteger numPaths = spl.getShortestPathCount();
                        double dep = 0;
                        for (String predecessor : spl.getPredecessorPathCountMap().keySet()) {
                            this.sendMessage(new Text(predecessor), PathData.getDependencyMessage(src, dep, numPaths));
                        }
                    }
                    noSuccessor.clear();
                }

                if (builder.length() > 1) builder.deleteCharAt(builder.length() - 1);
                logger.trace("ID: {} Step: {} State: {} set noSuccessor {}", id, step, state, builder);
                break;
            // continue the pair dependency phase until no updates are done / all dependencies are accumulated.
            case PAIR_DEPENDENCY_RUN:
                for (PathData message : messages) {
                    String src = message.getSource();

                    if (src.equals(id)) {
                        continue; // don't process paths for which you are the source
                    }

                    double successorDep = message.getDependency();
                    BigInteger successorNumPaths = message.getNumPaths();
                    BigInteger numPaths = vertexValue.getPathDataMap().get(src).getShortestPathCount();
                    double partialDep = (numPaths.doubleValue() / successorNumPaths.doubleValue()) * (1 + successorDep);
                    logger.debug("ID: {} Step: {} message [src:{} successorPaths:{} successorDep:{}] calculated [paths:{}, dep:{}]", id, step, src, successorNumPaths, successorDep, numPaths, partialDep);

                    // accumulate the dependency and subtract one successor
                    PartialDependency partialSum = vertexValue.addPartialDep(src, partialDep, -1);

                    // if the successor count reaches 0 pass this vertex dependency on to predecessors
                    if (partialSum.getSuccessors() == 0) {
                        ShortestPathList spl = vertexValue.getPathDataMap().get(src);
                        builder = new StringBuilder();
                        this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));
                        for (String predecessor : spl.getPredecessorPathCountMap().keySet()) {
                            this.sendMessage(new Text(predecessor), PathData.getDependencyMessage(src, partialSum.getDependency(), numPaths));
                            builder.append(predecessor).append(",");
                        }
                        if (builder.length() > 1) builder.deleteCharAt(builder.length() - 1);
                        logger.debug("ID: {} Step: {} forwarding partial dep to predecessors ({}) [src: {}, paths:{}, dep:{}]", id, step, builder, src, numPaths, partialSum.getDependency());
                    }
                }
                break;
            // completed dependency accumulation. calculate current betweenness value and clear all other vertex data
            // to prepare for next stage (which will be SHORTEST_PATH_START or the computation will halt, based on high betweenness
            // set stability, as determined in the master compute class.
            case PAIR_DEPENDENCY_COMPLETE:
                double approxBetweenness = vertexValue.getApproxBetweenness();
                for (PartialDependency partialDependency : vertexValue.getPartialDependencyMap().values()) {
                    approxBetweenness += partialDependency.getDependency();
                }
                logger.debug("ID: {} has betweenness value of {}", vertex.getId(), approxBetweenness);
                vertexValue.setApproxBetweenness(approxBetweenness);
                vertexValue.getPartialDependencyMap().clear();
                vertexValue.getPathDataMap().clear();
                this.aggregate(HBSEMasterCompute.HIGH_BC_SET_AGG, getNewHighBetweennessList(id, approxBetweenness));
                break;
        }
    }

    /**
     * Parses the Edge Value from a string.
     *
     * @param s Edge value as a string
     * @return long edge value
     */
    private BigInteger getEdgeValue(String s) {
        long edgeValue;
        try {
            edgeValue = Long.parseLong(s);
        } catch (NumberFormatException ex) {
            edgeValue = 1;
        }
        return BigInteger.valueOf(edgeValue);
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
            size = Integer.parseInt(getConf().get(HBSEConfigurationConstants.BETWEENNESS_SET_MAX_SIZE));
        } catch (NumberFormatException e) {
            logger.error("betweenness.set.maxSize must be set to a valid int.");
            throw e;
        }
        return new HighBetweennessList(size, id, value);
    }

    /**
     * Determines if a vertex can be a pivot point.
     *
     * @param id Vertex Id
     * @return True if it is a pivot point.
     */
    private boolean isPossiblePivotPoint(String id) {
        Random random = getRandomWithSeed(HBSEConfigurationConstants.PIVOT_BATCH_RANDOM_SEED);
        double percentageCutoff = (double) ((IntWritable) getAggregatedValue(HBSEMasterCompute.PIVOT_COUNT)).get() / getTotalNumVertices();
        double randomNumber = random.nextDouble();
        boolean isPivot = randomNumber < percentageCutoff;
        logger.debug("Selected as a possible pivot: {}", id);
        return isPivot;
    }

    private boolean isPivotPoint(String id) {
        PivotList pivots = getAggregatedValue(HBSEMasterCompute.PIVOT_AGG);
        logger.debug("Pivots have been selected for computation!  The count is {}", pivots.getPivots().size());
        return pivots.getPivots().contains(id);
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
            logger.debug("Set random seed: {}", seed);
        }
        return random;
    }
}
