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
package com.soteradefense.dga.louvain.giraph;

import com.soteradefense.dga.DGALoggingUtil;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * Performs the BSP portion of the distributed louvain algorithm.
 * <p/>
 * The computation is completed as a series of repeated steps, movement is restricted to approximately half of the nodes on each cycle so a full pass requires two cycles
 * <p/>
 * <ol>
 * <li>
 * Each vertex receives community values from its community hub and sends its own community to its neighbors
 * </li>
 * <li>
 * Each vertex determines if it should move to a neighboring community or not and sends its information to its community hub
 * </li>
 * <li>
 * Each community hub re-calculates community totals and sends the updates to each community member
 * </li>
 * </ol>
 * <p/>
 * When the number of nodes that change communities stops decreasing for four (4) cycles or when the number of nodes that change reaches 0 the computation ends
 */
public class LouvainComputation extends AbstractComputation<Text, LouvainNodeState, LongWritable, LouvainMessage, LouvainMessage> {

    private static final Logger logger = LoggerFactory.getLogger(LouvainComputation.class);

    // constants used to register and lookup aggregators
    public static final String CHANGE_AGG = "change_aggregator";
    public static final String TOTAL_EDGE_WEIGHT_AGG = "total_edge_weight_aggregator";

    // It may be the case that splitting the aggregators to multiple aggregators
    // will improve performance, set actual.Q.aggregators to set the number,
    // defaults to 1
    public static final String ACTUAL_Q_AGG = "actual_q_aggregator";


    private void aggregateQ(Double q) {
        aggregate(ACTUAL_Q_AGG, new DoubleWritable(q));
    }

    @Override
    public void initialize(GraphState graphState, WorkerClientRequestProcessor<Text, LouvainNodeState, LongWritable> workerClientRequestProcessor, CentralizedServiceWorker<Text, LouvainNodeState, LongWritable> graphTaskManager, WorkerGlobalCommUsage workerGlobalCommUsage) {
        super.initialize(graphState, workerClientRequestProcessor, graphTaskManager, workerGlobalCommUsage);
        DGALoggingUtil.setDGALogLevel(this.getConf());
    }

    @Override
    public void compute(Vertex<Text, LouvainNodeState, LongWritable> vertex, Iterable<LouvainMessage> messages) throws IOException {

        long currentSuperstep = getSuperstep();
        int currentMinorstep = (int) (currentSuperstep % 3); // the step in this iteration
        int currentIteration = (int) (currentSuperstep / 3); // the current iteration, two iterations make a full pass.

        //logger.info("currentSuperstep: " + currentSuperstep + " currentMinorstep: " + currentMinorstep + "currentIteration: " + currentIteration);
        
        LouvainNodeState vertexValue = vertex.getValue();

        // count the total edge weight of the graph on the first super step only
        if (currentSuperstep == 0) {
            if (!vertexValue.isFromLouvainVertexReader()) {
                vertexValue.setCommunity(vertex.getId().toString());
                long edgeWeightAggregation = 0;
                for (Edge<Text, LongWritable> edge : vertex.getEdges()) {
                    edgeWeightAggregation += edge.getValue().get();
                }
                vertexValue.setNodeWeight(edgeWeightAggregation);
            }
            aggregate(TOTAL_EDGE_WEIGHT_AGG, new LongWritable(vertexValue.getNodeWeight() + vertexValue.getInternalWeight()));
        } else if (vertexValue.getCommunity().equals("")) {
            vertexValue.setCommunity(vertex.getId().toString());
            vertexValue.setNodeWeight(0L);
        }

        if (currentSuperstep == 0 && vertex.getNumEdges() == 0) {
            // nodes that have no edges send themselves a message on the step 0
            this.sendMessage(vertex.getId(), new LouvainMessage());
            vertex.voteToHalt();
            return;
        } else if (currentSuperstep == 1 && vertex.getNumEdges() == 0) {
            // nodes that have no edges aggregate their Q value and exit computation on step 1
            double q = calculateActualQ(vertex, new ArrayList<LouvainMessage>());
            aggregateQ(q);
            vertex.voteToHalt();
            return;
        }

        // at the start of each full pass check to see if progress is still being made, if not halt
        if (currentMinorstep == 1 && currentIteration > 0 && currentIteration % 2 == 0) {
            vertexValue.setChanged(0L); // change count is per pass
            long totalChange = ((LongWritable) getAggregatedValue(CHANGE_AGG)).get();
            vertexValue.getChangeHistory().add(totalChange);

            // if halting aggregate q value and replace node edges with community edges (for next stage in pipeline)
            if (LouvainMasterCompute.decideToHalt(vertexValue.getChangeHistory(), getConf())) {
                double q = calculateActualQ(vertex, messages);
                replaceNodeEdgesWithCommunityEdges(vertex, messages);
                aggregateQ(q);
                return;
                // note: we did not vote to halt, MasterCompute will halt computation on next step
            }
        }

        try {
            switch (currentMinorstep) {
                case 0:
                    getAndSendCommunityInfo(vertex, messages);

                    // if the next step well require a progress check, aggregate the number of nodes who have changed community.
                    if (currentIteration > 0 && currentIteration % 2 == 0) {
                        aggregate(CHANGE_AGG, new LongWritable(vertexValue.getChanged()));
                    }

                    break;
                case 1:
                    calculateBestCommunity(vertex, messages, currentIteration);
                    break;
                case 2:
                    updateCommunities(vertex, messages);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid minorstep: " + currentMinorstep);
            }
        } finally {
            vertex.voteToHalt();
        }

    }


    /**
     * Get the total edge weight of the graph.
     *
     * @return 2*the total graph weight.
     */
    private long getTotalEdgeWeight() {
        long m = ((LongWritable) getAggregatedValue(TOTAL_EDGE_WEIGHT_AGG)).get();
        return m;
    }

    /**
     * Each vertex will recieve its own communities sigma_total (if updated),
     * and then send its currenty community info to each of its neighbors.
     *
     * @param messages
     */
    private void getAndSendCommunityInfo(Vertex<Text, LouvainNodeState, LongWritable> vertex, Iterable<LouvainMessage> messages) {
        LouvainNodeState state = vertex.getValue();
        // set new community information.
        if (getSuperstep() > 0) {
            Iterator<LouvainMessage> it = messages.iterator();
            if (!it.hasNext()) {
                throw new IllegalStateException("No community info recieved in getAndSendCommunityInfo! Superstep: " + getSuperstep() + " id: " + vertex.getId());
            }
            LouvainMessage inMess = it.next();
            if (it.hasNext()) {
                throw new IllegalStateException("More than one community info packets recieved in getAndSendCommunityInfo! Superstep: " + getSuperstep() + " id: " + vertex.getId());
            }
            state.setCommunity(inMess.getCommunityId());
            state.setCommunitySigmaTotal(inMess.getCommunitySigmaTotal());
        }

        // send community info to all neighbors
        for (Edge<Text, LongWritable> edge : vertex.getEdges()) {
            LouvainMessage outMess = new LouvainMessage();
            outMess.setCommunityId(state.getCommunity());
            outMess.setCommunitySigmaTotal(state.getCommunitySigmaTotal());
            outMess.setEdgeWeight(edge.getValue().get());
            outMess.setSourceId(vertex.getId().toString());
            this.sendMessage(edge.getTargetVertexId(), outMess);
        }

    }

    /**
     * Based on community of each of its neighbors, each vertex determimnes if
     * it should retain its currenty community or swtich to a neighboring
     * communinity.
     * <p/>
     * At the end of this step a message is sent to the nodes community hub so a
     * new community sigma_total can be calculated.
     *
     * @param messages
     * @param iteration
     */
    private void calculateBestCommunity(Vertex<Text, LouvainNodeState, LongWritable> vertex, Iterable<LouvainMessage> messages, int iteration) {

        LouvainNodeState state = vertex.getValue();

        // group messages by communities.
        HashMap<String, LouvainMessage> communityMap = new HashMap<String, LouvainMessage>();
        for (LouvainMessage message : messages) {

            String communityId = message.getCommunityId();
            long weight = message.getEdgeWeight();
            LouvainMessage newmess = new LouvainMessage(message);

            if (communityMap.containsKey(communityId)) {
                LouvainMessage m = communityMap.get(communityId);
                m.setEdgeWeight(m.getEdgeWeight() + weight);
            } else {
                communityMap.put(communityId, newmess);
            }
        }

        // calculate change in Q for each potential community
        String bestCommunityId = vertex.getValue().getCommunity();
        String startingCommunityId = bestCommunityId;
        BigDecimal maxDeltaQ = new BigDecimal("0.0");
        for (Map.Entry<String, LouvainMessage> entry : communityMap.entrySet()) {
            BigDecimal deltaQ = calculateQDelta(startingCommunityId, entry.getValue().getCommunityId(), entry.getValue().getCommunitySigmaTotal(), entry.getValue().getEdgeWeight(), state.getNodeWeight(), state.getInternalWeight());
            if (deltaQ.compareTo(maxDeltaQ) > 0 || (deltaQ.equals(maxDeltaQ) && entry.getValue().getCommunityId().compareTo(bestCommunityId) < 0)) {
                bestCommunityId = entry.getValue().getCommunityId();
                maxDeltaQ = deltaQ;
            }
        }

        // ignore switches based on iteration (prevent certain cycles)
        if ((state.getCommunity().compareTo(bestCommunityId) > 0 && iteration % 2 == 0) || (state.getCommunity().compareTo(bestCommunityId) < 0 && iteration % 2 != 0)) {
            bestCommunityId = state.getCommunity();
            // System.out.println("Iteration: "+iteration+" Node: "+getId()+" held stable to prevent cycle");
        }

        // update community and change count
        if (!state.getCommunity().equals(bestCommunityId)) {
            String old = state.getCommunity();
            LouvainMessage c = communityMap.get(bestCommunityId);
            if (!bestCommunityId.equals(c.getCommunityId())) {
                throw new IllegalStateException("Community mapping contains wrong Id");
            }
            state.setCommunity(c.getCommunityId());
            state.setCommunitySigmaTotal(c.getCommunitySigmaTotal());
            state.setChanged(1L);
            logger.debug("Iteration: {} Node: {} changed from {} -> {} dq: {}", iteration, vertex.getId(), old, state.getCommunity(), maxDeltaQ);
        }

        // send our node weight to the community hub to be summed in next
        // superstep
        this.sendMessage(new Text(state.getCommunity()), new LouvainMessage(state.getCommunity(), state.getNodeWeight() + state.getInternalWeight(), 0, vertex.getId().toString()));
    }

    /**
     * determine the change in q if a node were to move to the given community.
     *
     * @param currCommunityId
     * @param testCommunityId
     * @param testSigmaTotal
     * @param edgeWeightInCommunity (sum of weight of edges from this node to target community)
     * @param nodeWeight            (the node degree)
     * @param internalWeight
     * @return
     */
    private BigDecimal calculateQDelta(String currCommunityId, String testCommunityId, long testSigmaTotal, long edgeWeightInCommunity, long nodeWeight, long internalWeight) {
        boolean isCurrentCommunity = (currCommunityId.equals(testCommunityId));
        BigDecimal M = new BigDecimal(Long.toString(getTotalEdgeWeight()));
        long k_i_in_L = (isCurrentCommunity) ? edgeWeightInCommunity + internalWeight : edgeWeightInCommunity;
        BigDecimal k_i_in = new BigDecimal(Long.toString(k_i_in_L));
        BigDecimal k_i = new BigDecimal(Long.toString(nodeWeight + internalWeight));
        BigDecimal sigma_tot = new BigDecimal(Long.toString(testSigmaTotal));
        if (isCurrentCommunity) {
            sigma_tot = sigma_tot.subtract(k_i);
        }

        BigDecimal deltaQ = new BigDecimal("0.0");
        if (!(isCurrentCommunity && sigma_tot.equals(deltaQ))) {
            BigDecimal dividend = k_i.multiply(sigma_tot);
            int scale = 20;
            deltaQ = k_i_in.subtract(dividend.divide(M, scale, RoundingMode.HALF_DOWN));

        }
        return deltaQ;
    }

    /**
     * Each community hub aggregates the values from each of its members to
     * update the node's sigma total, and then sends this back to each of its
     * members.
     *
     * @param messages
     */
    private void updateCommunities(Vertex<Text, LouvainNodeState, LongWritable> vertex, Iterable<LouvainMessage> messages) {
        // sum all community contributions
        LouvainMessage sum = new LouvainMessage();
        sum.setCommunityId(vertex.getId().toString());
        sum.setCommunitySigmaTotal(0);
        for (LouvainMessage m : messages) {
            sum.addToSigmaTotal(m.getCommunitySigmaTotal());
        }

        // send community back out to all community members
        for (LouvainMessage m : messages) {
            this.sendMessage(new Text(m.getSourceId()), sum);
        }
    }

    /**
     * Calculate this nodes contribution for the actual q value of the graph.
     */
    private double calculateActualQ(Vertex<Text, LouvainNodeState, LongWritable> vertex, Iterable<LouvainMessage> messages) {
        // long start = System.currentTimeMillis();
        LouvainNodeState state = vertex.getValue();
        long k_i_in = state.getInternalWeight();
        for (LouvainMessage m : messages) {
            if (m.getCommunityId().equals(state.getCommunity())) {
                try {
                    k_i_in += vertex.getEdgeValue(new Text(m.getSourceId())).get();
                } catch (NullPointerException e) {
                    throw new IllegalStateException("Node: " + vertex.getId() + " does not have edge: " + m.getSourceId() + "  check that the graph is bi-directional.");
                }
            }
        }
        long sigma_tot = vertex.getValue().getCommunitySigmaTotal();
        long M = this.getTotalEdgeWeight();
        long k_i = vertex.getValue().getNodeWeight() + vertex.getValue().getInternalWeight();

        double q = ((((double) k_i_in) / M) - (((double) (sigma_tot * k_i)) / Math.pow(M, 2)));
        q = (q < 0) ? 0 : q;

        // long end = System.currentTimeMillis();
        // System.out.println("calculated actual q in :"+(end-start));
        return q;
    }

    /**
     * Replace each edge to a neighbor with an edge to that neigbors community
     * instead. Done just before exiting computation. In the next state of the
     * piple line this edges are aggregated and all communities are represented
     * as single nodes. Edges from the community to itself are tracked be the
     * ndoes interal weight.
     *
     * @param messages
     */
    private void replaceNodeEdgesWithCommunityEdges(Vertex<Text, LouvainNodeState, LongWritable> vertex, Iterable<LouvainMessage> messages) {

        // group messages by communities.
        HashMap<String, LouvainMessage> communityMap = new HashMap<String, LouvainMessage>();
        for (LouvainMessage message : messages) {

            String communityId = message.getCommunityId();

            if (communityMap.containsKey(communityId)) {
                LouvainMessage m = communityMap.get(communityId);
                m.setEdgeWeight(m.getEdgeWeight() + message.getEdgeWeight());
            } else {
                LouvainMessage newmess = new LouvainMessage(message);
                communityMap.put(communityId, newmess);
            }
        }

        List<Edge<Text, LongWritable>> edges = new ArrayList<Edge<Text, LongWritable>>(communityMap.size() + 1);
        for (Map.Entry<String, LouvainMessage> entry : communityMap.entrySet()) {
            edges.add(EdgeFactory.create(new Text(entry.getKey()), new LongWritable(entry.getValue().getEdgeWeight())));
        }
        vertex.setEdges(edges);
    }

}
