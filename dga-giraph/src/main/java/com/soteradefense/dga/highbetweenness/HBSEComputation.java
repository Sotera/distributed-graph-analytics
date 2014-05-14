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
package com.soteradefense.dga.highbetweenness;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * "W. Chong, Efficient Extraction of High-Betweenness Vertices"
 * <p/>
 * For background information on the method of accumulation of pair dependencies and shortest path data see:
 * "U. Brandes, A Faster Algorithm for Betweenness Centrality"
 */
public class HBSEComputation extends AbstractComputation<Text, VertexData, Text, PathData, PathData> {

    private static final Log LOG = LogFactory.getLog(HBSEComputation.class);

    /**
     * Works in two major stages which are repeated and coordinated
     * by the setting of a global state (set by the master compute class)
     * <p/>
     * Stage 1:  Discover shortest paths, and shortest path counts for each
     * source vertex in a globally set pivot batch to each node in the graph.
     * Stage 2:  Accumulate pair dependencies
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
            if (!(step == 0)) {
                setGlobalPivots(getPivotBatch(), HBSEMasterCompute.PREVIOUS_PIVOT_AGG);
            }
            if (isPivotPoint(id)) {
                LOG.info("Superstep: " + step + " Start new shortest path computation. Source = " + id);
                for (Edge<Text, Text> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), PathData.getShortestPathMessage(id, id, Long.parseLong(edge.getValue().toString()), 1));
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
                long numPaths = spl.getNumShortestPaths();
                updateCount++;
                for (Edge<Text, Text> edge : vertex.getEdges()) {
                    long newDistance = spl.getDistance() + Long.parseLong(edge.getValue().toString());
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
                    for (String pred : entry.getValue().getPredPathCountMap().keySet()) {
                        this.sendMessage(new Text(pred), PathData.getPingMessage(source));
                        builder.append("(").append(pred).append(",").append(source).append("),");
                    }
                }

            }
            if (builder.length() > 1) builder.deleteCharAt(builder.length() - 1);
            LOG.trace("ID: " + id + " Step: " + step + " State: " + state + " sent messages (pred,source):  " + builder.toString());
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
            allPaths.remove(vertex.getId());
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
                    long numPaths = spl.getNumShortestPaths();
                    double dep = 0;
                    for (String pred : spl.getPredPathCountMap().keySet()) {
                        this.sendMessage(new Text(pred), PathData.getDependencyMessage(src, dep, numPaths));
                    }
                }
                noSuccessor.clear();
            }

            if (builder.length() > 1) builder.deleteCharAt(builder.length() - 1);
            LOG.trace("ID: " + id + " Step: " + step + " State: " + state + " set noSuccessor " + builder.toString());

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
                long numPaths = vertexValue.getPathDataMap().get(src).getNumShortestPaths();
                double partialDep = ((double) numPaths / successorNumPaths) * (1 + successorDep);
                LOG.debug("ID: " + id + " Step: " + step + " message {src:" + src + " successorPaths:" + successorNumPaths + " successorDep:" + successorDep + "} calculated {paths:" + numPaths + ", dep:" + partialDep + "}");

                // accumulate the dependency and subtract one successor
                PartialDependency partialSum = vertexValue.addPartialDep(src, partialDep, -1);

                // if the successor count reaches 0 pass this vertex dependency on to predecessors
                if (partialSum.getSuccessors() == 0) {
                    ShortestPathList spl = vertexValue.getPathDataMap().get(src);
                    StringBuilder builder = new StringBuilder();
                    this.aggregate(HBSEMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));
                    for (String pred : spl.getPredPathCountMap().keySet()) {
                        this.sendMessage(new Text(pred), PathData.getDependencyMessage(src, partialSum.getDependency(), numPaths));
                        builder.append(pred).append(",");
                    }
                    if (builder.length() > 1) builder.deleteCharAt(builder.length() - 1);
                    LOG.debug("ID: " + id + " Step: " + step + " forwarding partial dep to predecessors (" + builder.toString() + ") {src:" + src + ", paths:" + numPaths + ", dep:" + partialSum.getDependency() + "}");
                }
            }
        }


        // completed dependency accumulation. calculate current betweenness value and clear all other vertex data
        // to prepare for next stage (which will be SHORTEST_PATH_START or the computation will halt, based on high betweenness
        // set stability, as determined in the master compute class.
        if (State.PAIR_DEPENDENCY_COMPLETE == state) {
            double approxBetweenness = vertexValue.getApproxBetweenness();
            for (PartialDependency partialDep : vertexValue.getPartialDependencyMap().values()) {
                approxBetweenness += partialDep.getDependency();
            }
            vertexValue.setApproxBetweenness(approxBetweenness);
            vertexValue.getPartialDependencyMap().clear();
            vertexValue.getPathDataMap().clear();
            this.aggregate(HBSEMasterCompute.HIGH_BC_SET_AGG, getNewHighBetweennessList(id, approxBetweenness));
        }


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
     * Read the global pivot batch:  the set of vertices that are to be used
     * as sources in this phase.
     *
     * @return The current batch of pivot points.
     */
    private Set<String> getPivotBatch() {
        TextArrayWritable iwa = this.getAggregatedValue(HBSEMasterCompute.PIVOT_AGG);
        return getBatch(iwa);
    }

    private Set<String> getBatch(TextArrayWritable iwa) {
        return iwa.getPivots();
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
        int size = 1;
        try {
            size = Integer.parseInt(getConf().get(HBSEMasterCompute.BETWEENNESS_SET_MAX_SIZE));
        } catch (NumberFormatException e) {
            LOG.error("betweenness.set.maxSize must be set to a valid int.");
            throw e;
        }
        return new HighBetweennessList(size, id, value);
    }

    private boolean isPivotPoint(String id) {
        Random random = getRandomWithSeed(HBSEMasterCompute.PIVOT_BATCH_RANDOM_SEED);
        double percentageCutoff;
        if (getSuperstep() == 0) {
            percentageCutoff = ((DoubleWritable) getAggregatedValue(HBSEMasterCompute.INITIAL_PIVOT_PERCENT)).get();
        } else {
            percentageCutoff = ((DoubleWritable) getAggregatedValue(HBSEMasterCompute.PIVOT_PERCENT)).get();
        }
        double randomNumber = random.nextDouble();
        boolean isPivot = randomNumber < percentageCutoff;
        if (isPivot) {
            isPivot = !setPivot(id);
        }
        return isPivot;
    }

    private boolean setPivot(String id) {
        boolean wasPreviouslyUsed = true;
        Set<String> previousBatch = getPreviousPivotBatch();
        if (!previousBatch.contains(id)) {
            Set<String> currentPivots = getPivotBatch();
            currentPivots.add(id);
            setGlobalPivots(currentPivots, HBSEMasterCompute.PIVOT_AGG);
            wasPreviouslyUsed = false;
        }
        return wasPreviouslyUsed;
    }

    private void setGlobalPivots(Collection<String> pivots, String name) {
        this.aggregate(name, new TextArrayWritable(pivots));
    }

    private Set<String> getPreviousPivotBatch() {
        TextArrayWritable previousPivots = getAggregatedValue(HBSEMasterCompute.PREVIOUS_PIVOT_AGG);
        return getBatch(previousPivots);
    }

    private Random getRandomWithSeed(String name) {
        Random random;
        String randomSeedStr = getConf().get(name);
        if (randomSeedStr == null) {
            random = new Random();
        } else {
            long seed = Long.parseLong(randomSeedStr);
            random = new Random(seed);
            LOG.info("Set random seed: " + seed);
        }
        return random;
    }
}
