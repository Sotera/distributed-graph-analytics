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
import org.apache.giraph.aggregators.DoubleOverwriteAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;


/**
 * Coordinates Global state for the SBVertex to calculate shortest paths, accumulates pair dependency information,
 * and monitor high betweenness set stability.
 * <p/>
 * Required configuration settings:
 * <ul>
 * <li>fs.defaultFS OR fs.default.name:  If not set in the environment you can set them as a custom arguments. typically this will not need to be set. </li>
 * <li>betweenness.output.dir: Directory in HDFS used to write the high betweenness set.</li>
 * <li>betweenness.set.stability: Integer value, algorithm completes with the high betweenness set changes by less than this value, checked after each cycle.</li>
 * <li>betweenness.set.stability.counter: Integer value, number of times the stability threshold must be reached.</li>
 * <li>betweenness.set.maxSize: Size the result set desired.</li>
 * <li>pivot.batch.size: Number of pivots to use in each batch</li>
 * <li>pivot.batch.size.initial:  Number of pivots to use in the first batch (defaults to pivot.batch.size)</li>
 * <li>vertex.count: The number of vertices to be loaded</li>
 * <li>betweenness.shortest.path.phases: Number of shortest path phases to run for every 1 dependency accumulation phase.</li>
 * </ul>
 * <p/>
 * WARNING:  id values for all vertices must be in 0,1,2,...N where N=vertex.count-1
 * <p/>
 * For background information on the approximation method see:
 * <a href="http://www.google.com/url?url=http://scholar.google.com/scholar_url%3Fhl%3Den%26q%3Dhttp://www.researchgate.net/publication/221273491_Efficient_Extraction_of_High-Betweenness_Vertices/file/3deec52a5dd8a6faa1.pdf%26sa%3DX%26scisig%3DAAGBfm1Xl41dnryyDhAGnt9AYOL6iHLoOg%26oi%3Dscholarr&rct=j&q=&esrc=s&sa=X&ei=j15yU7mwGMKdyATX7ID4BA&ved=0CC0QgAMoADAA&usg=AFQjCNH-dTuG7bYZk4__IQGwGvFrnQ9mGQ&cad=rja">"W. Chong, Efficent Extraction of High-Betweenness Vertices"</a>
 * <p/>
 * For background information on the method of accumulation of pair dependencies and shortest path data see:
 * <a href="http://www.google.com/url?url=http://scholar.google.com/scholar_url%3Fhl%3Den%26q%3Dhttp://kops.ub.uni-konstanz.de/bitstream/handle/urn:nbn:de:bsz:352-opus-71888/algorithm.pdf%253Fsequence%253D1%26sa%3DX%26scisig%3DAAGBfm2tszb3JWsE0Mp8E5os2p-udyVKtw%26oi%3Dscholarr&rct=j&q=&esrc=s&sa=X&ei=4l5yU7CuOsOPyAS-7YLoBw&ved=0CCgQgAMoADAA&usg=AFQjCNGKQ_j2h7QQSQncFUGgkpKO4Uo3Yw&cad=rja">"U. Brandes, A Faster Algorithm for Betweenness Centrality"</a>
 */
public class HBSEMasterCompute extends DefaultMasterCompute {

    private static final Log LOG = LogFactory.getLog(HBSEMasterCompute.class);

    /**
     * Aggregator Identifier that gets the state of the computation.
     */
    public static final String STATE_AGG = "com.sotera.graph.singbetweenness.STATE_AGG";
    /**
     * Aggregator Identifier for the global pivot points.
     */
    public static final String PIVOT_AGG = "com.sotera.graph.singbetweenness.PIVOT_AGG";
    public static final String PREVIOUS_PIVOT_AGG = PIVOT_AGG + "_PREVIOUS";
    /**
     * Aggregator Identifier for the number of nodes changed in the highbetweenness list comparison.
     */
    public static final String UPDATE_COUNT_AGG = "com.sotera.graph.singbetweenness.UPDATE_COUNT_AGG";
    /**
     * Aggregator Identifier for the saved highbetweenness set.
     */
    public static final String HIGH_BC_SET_AGG = "com.sotera.graph.singbetweenness.HIGH_BC_SET_AGG";
    /**
     * Configuration Identifier for the directory to output the highbetweenness set.
     */
    public static final String BETWEENNESS_OUTPUT_DIR = "betweenness.output.dir";
    /**
     * Configuration Identifier for the number of shortest path phases to run through.
     */
    public static final String BETWEENNESS_SHORTEST_PATH_PHASES = "betweenness.shortest.path.phases";
    /**
     * Configuration Identifier for the set stability cut off point (margin of error).
     */
    public static final String BETWEENNESS_SET_STABILITY = "betweenness.set.stability";
    /**
     * Configuration Identifier for the set stability counter cut off point (margin of error).
     */
    public static final String BETWEENNESS_SET_STABILITY_COUNTER = BETWEENNESS_SET_STABILITY + ".counter";
    /**
     * Configuration Identifier for the maximum number of nodes in the betweenness set.
     */
    public static final String BETWEENNESS_SET_MAX_SIZE = "betweenness.set.maxSize";
    /**
     * Configuration Identifier for the pivot point batch size.
     */
    public static final String PIVOT_BATCH_SIZE = "pivot.batch.size";
    /**
     * Configuration Identifier for the initial pivot point batch size.
     */
    public static final String PIVOT_BATCH_SIZE_INITIAL = PIVOT_BATCH_SIZE + ".initial";
    /**
     * Configuration Identifier for the random seed value when choosing new pivot points.
     */
    public static final String PIVOT_BATCH_RANDOM_SEED = "pivot.batch.random.seed";
    /**
     * Configuration Identifier for the number of vertices to perform the operation on.
     */
    public static final String VERTEX_COUNT = "vertex.count";
    /**
     * Configuration Identifier for the starting pivot points (Comma Separated).
     */
    public static final String PIVOT_BATCH_STRING = "pivot.batch.string";
    /**
     * Configuration Identifier for the default file system.
     */
    public static final String FS_DEFAULT_FS = "fs.defaultFS";
    /**
     * Configuration Identifier for the default name.
     */
    public static final String FS_DEFAULT_NAME = "fs.default.name";
    /**
     * This is the filename for the final highbetweenness set
     */
    public static final String FINAL_SET_CSV = "final_set.csv";
    /**
     * The filename where the stats are written
     */
    public static final String STATS_CSV = "stats.csv";
    public static final String PIVOT_BATCH_DELIMITER = ",";
    public static final String PIVOT_PERCENT = "pivot.percent.cutoff";
    public static final String INITIAL_PIVOT_PERCENT = PIVOT_PERCENT + ".initial";


    /**
     * Stores the number of pivots to use per batch of nodes.
     */
    private int batchSize;

    /**
     * Stores the number of shortest path phases to run through before completion.
     */
    private int shortestPathPhases;
    /**
     * Stores the total number of shortest path phases completed at a certain period of computation.
     */
    private int shortestPathPhasesCompleted;

    /**
     * Stores the total number of vertices to use in the period of computation.
     */
    private int maxId;

    /**
     * Stores the current set of highbetweenness nodes.
     */
    private Set<String> highBetweennessSet = new HashSet<String>();

    /**
     * Current cycle: a cycle is defined here as a shortest path phase + a pair dependency phase
     */
    private int cycle = 1;

    /**
     * Stores the stability cut off value after a full cycle.
     */
    private int stabilityCutoff;
    /**
     * Stores the number of times the stability counter must be met before exiting.
     */
    private int stabilityCounter;
    /**
     * Stores the running count of the number of times the stability counter was met.
     */
    private int stabilityRunningCounter = 0;

    /**
     * Output directory in HDFS.
     */
    private String outputDir;


    /**
     * Variable that tracks the current state of computation.
     */
    private State state = State.START;

    /**
     * Date variable to track the start entire computation.
     */
    private Date start;
    /**
     * Date variable to track the end entire computation.
     */
    private Date end;

    /**
     * Read options from configuration file and set up aggregators (global communication)
     *
     * @throws InstantiationException if it is unable to register an aggregator with a specific class.
     * @throws IllegalAccessException if it is unable to access the class passed in as an aggregator.
     */
    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        start = new Date();
        state = State.START;
        this.registerPersistentAggregator(STATE_AGG, IntOverwriteAggregator.class);
        this.registerPersistentAggregator(PIVOT_AGG, PivotSetAggregator.class);
        this.registerPersistentAggregator(PREVIOUS_PIVOT_AGG, PivotSetAggregator.class);
        this.registerPersistentAggregator(PIVOT_PERCENT, DoubleOverwriteAggregator.class);
        this.registerPersistentAggregator(INITIAL_PIVOT_PERCENT, DoubleOverwriteAggregator.class);
        this.registerAggregator(UPDATE_COUNT_AGG, IntSumAggregator.class);
        this.registerAggregator(HIGH_BC_SET_AGG, HighBCSetAggregator.class);

        String defaultFS = this.getDefaultFS(this.getConf());
        if (defaultFS == null) {
            throw new IllegalArgumentException(FS_DEFAULT_FS + " OR " + FS_DEFAULT_NAME + " must be set.  If not set in the environment you can set them as a custom argument to the giraph job via -ca fs.default.name=<your default fs>");
        }

        outputDir = getConf().get(BETWEENNESS_OUTPUT_DIR);
        if (outputDir == null || outputDir.length() < 1) {
            throw new IllegalArgumentException(BETWEENNESS_OUTPUT_DIR + " must be set to a valid directory in HDFS");
        }

        shortestPathPhases = getOptionalHBSEConfiguration(BETWEENNESS_SHORTEST_PATH_PHASES, 1);
        this.shortestPathPhasesCompleted = 0;

        stabilityCutoff = getOptionalHBSEConfiguration(BETWEENNESS_SET_STABILITY, 0);
        LOG.info(BETWEENNESS_SET_STABILITY + "=" + stabilityCutoff);

        stabilityCounter = getOptionalHBSEConfiguration(BETWEENNESS_SET_STABILITY_COUNTER, 3);
        LOG.info(HBSEMasterCompute.BETWEENNESS_SET_STABILITY_COUNTER + "=" + stabilityCounter);

        int maxHighBCSetSize = getRequiredHBSEConfiguration(BETWEENNESS_SET_MAX_SIZE);
        LOG.info(HBSEMasterCompute.BETWEENNESS_SET_MAX_SIZE + "=" + maxHighBCSetSize);


        // manually set first pivot batch if argument is present
        LinkedList<String> currentPivots = new LinkedList<String>();
        try {
            String pivotBatchStr = this.getConf().get(PIVOT_BATCH_STRING);
            if (pivotBatchStr != null && pivotBatchStr.length() > 0) {
                String[] pivotBatchArray = pivotBatchStr.split(PIVOT_BATCH_DELIMITER);
                for (String pivotStr : pivotBatchArray) {
                    currentPivots.add(pivotStr);
                    LOG.info("Manually added pivot: " + pivotStr);
                }
            }

        } catch (NumberFormatException e) {
            LOG.error("Optional argument " + PIVOT_BATCH_STRING + " invalid. Must be a comma separated list of Ids.");
            throw e;
        }
        setGlobalPivots(currentPivots);

        batchSize = getRequiredHBSEConfiguration(PIVOT_BATCH_SIZE);
        LOG.info(PIVOT_BATCH_SIZE + "=" + batchSize);


        int initialBatchSize = getOptionalHBSEConfiguration(PIVOT_BATCH_SIZE_INITIAL, batchSize);

        this.setAggregatedValue(PREVIOUS_PIVOT_AGG, new PivotSetWritable());
        maxId = getRequiredHBSEConfiguration(VERTEX_COUNT);
        this.setAggregatedValue(PIVOT_PERCENT, new DoubleWritable((double) batchSize / maxId));
        this.setAggregatedValue(INITIAL_PIVOT_PERCENT, new DoubleWritable((double) initialBatchSize / maxId));
        LOG.info(VERTEX_COUNT + "=" + maxId);

    }

    /**
     * Gets a Require Configuration value.
     *
     * @param name The configuration name to get.
     * @return A valid integer
     * @throws java.lang.NumberFormatException if it can't be parsed to an int.
     */
    private int getRequiredHBSEConfiguration(String name) {
        String propValue = getConf().get(name);
        try {
            return Integer.parseInt(propValue);
        } catch (NumberFormatException e) {
            LOG.error("Option not set or invalid. \"" + name + "\" must be set to a valid int, was set to: " + propValue, e);
            throw e;
        }
    }

    /**
     * Gets an optional configuration value and only logs a NumberFormatException.
     *
     * @param name         The configuration name to get.
     * @param defaultValue The default value to be returned if there is an error.
     * @return Integer Configuration Value
     */
    private int getOptionalHBSEConfiguration(String name, int defaultValue) {
        String propValue = getConf().get(name);
        try {
            if (propValue == null)
                return defaultValue;
            return Integer.parseInt(propValue);
        } catch (NumberFormatException e) {
            LOG.error("Option not set or invalid. \"" + name + "\" must be set to a valid int, was set to: " + defaultValue, e);
            return defaultValue;
        }
    }

    /**
     * Coordinates the computation phases of SBVertex by monitoring for the completion of each state
     * and moving to the next state.
     * <ol>
     * <li>selects pivots</li>
     * <li>monitors for completion of shortest paths</li>
     * <li>starts pair dependency phase</li>
     * <li>monitors for completion of pair dependency</li>
     * <li>checks high betweenness set stability</li>
     * <li>if set is stable save set and exit else select new pivots and start new shortest path phase</li>
     * </ol>
     */
    @Override
    public void compute() {
        long step = this.getSuperstep();
        LOG.info("Superstep: " + step + " starting in State: " + state);
        switch (state) {
            case START:
                state = State.SHORTEST_PATH_START;
                setGlobalState(state);
                LOG.info("Superstep: " + step + " Switched to State: " + state);
                break;
            case SHORTEST_PATH_START:
                int updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
                LOG.info("Superstep: " + step + " Paths updated: " + updateCount);
                state = State.SHORTEST_PATH_RUN;
                setGlobalState(state);
                LOG.info("Superstep: " + step + " Switched to State: " + state);
                break;
            case SHORTEST_PATH_RUN:
                updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
                LOG.info("Superstep: " + step + " Paths updated: " + updateCount);
                if (updateCount == 0) {
                    shortestPathPhasesCompleted++;
                    if (shortestPathPhasesCompleted == shortestPathPhases) {
                        state = State.PAIR_DEPENDENCY_PING_PREDECESSOR;
                    } else {
                        state = State.SHORTEST_PATH_START;
                    }
                    setGlobalState(state);
                    LOG.info("Superstep: " + step + " UPDATE COUNT 0, shortest path phase " + shortestPathPhasesCompleted + " of " + shortestPathPhases + " Switched to State: " + state);
                }
                break;
            case PAIR_DEPENDENCY_PING_PREDECESSOR:
                shortestPathPhasesCompleted = 0;
                state = State.PAIR_DEPENDENCY_FIND_SUCCESSORS;
                setGlobalState(state);
                LOG.info("Superstep: " + step + " Switched to State: " + state);
                break;
            case PAIR_DEPENDENCY_FIND_SUCCESSORS:
                state = State.PAIR_DEPENDENCY_RUN;
                setGlobalState(state);
                LOG.info("Superstep: " + step + " Switched to State: " + state);
                break;
            case PAIR_DEPENDENCY_RUN:
                updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
                if (updateCount == 0) {
                    state = State.PAIR_DEPENDENCY_COMPLETE;
                    setGlobalState(state);
                }
                LOG.info("Superstep: " + step + " UPDATE COUNT " + updateCount + ", State: " + state);
                break;
            case PAIR_DEPENDENCY_COMPLETE:
                HighBetweennessList hbl = getAggregatedValue(HIGH_BC_SET_AGG);
                Set<String> incomingSet = hbl.getHighBetweennessSet();
                int delta = this.compareHighBetweennessSet(incomingSet);
                highBetweennessSet = incomingSet;

                LOG.info("High Betweenness Set Delta: " + delta);
                String logprefix = "Finished Cycle: " + cycle;
                cycle++;

                if (delta <= stabilityCutoff) {
                    stabilityRunningCounter++;
                    if (stabilityRunningCounter >= stabilityCounter) {
                        LOG.info(logprefix + " Set Delta < cutoff value; counter=" + stabilityRunningCounter + " approximation complete.");
                        state = State.FINISHED;
                    } else {
                        LOG.info(logprefix + " Set Delta < cutoff value; counter=" + stabilityRunningCounter);
                        state = State.SHORTEST_PATH_START;
                    }

                } else if (getCurrentPivots().getPivots().size() + getPreviousPivots().getPivots().size() == this.maxId) {
                    LOG.info(logprefix + " All possible pivots selected, exiting");
                    state = State.FINISHED;
                } else {
                    stabilityRunningCounter = 0; // reset stabilityRunningCounter
                    LOG.info(logprefix + " Delta did not meet cutoff, starting next cycle.");
                    state = State.SHORTEST_PATH_START;
                }
                setGlobalState(state);
                LOG.info("Superstep: " + step + ", going to State: " + state);
                break;
            case FINISHED:
                this.haltComputation();
                end = new Date();
                this.writeHighBetweennessSet(highBetweennessSet);
                this.writeStats();
                break;
            default:
                LOG.error("INVALID STATE: " + state);
                throw new IllegalStateException("Invalid State" + state);
        }
    }

    /**
     * Writes the various statistics when computation finishes.
     */
    private void writeStats() {

        int pivotsSelected = getCurrentPivots().getPivots().size() + getPreviousPivots().getPivots().size();
        double percentSelected = (double) pivotsSelected / this.maxId;
        int time = (int) ((end.getTime() - start.getTime()) / 1000);

        String defaultFS = getDefaultFS(getConf());
        String filename = defaultFS + "/" + outputDir + "/" + STATS_CSV;
        Path pt = new Path(filename);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
            try {
                br.write("k: " + this.highBetweennessSet.size() + "\n");
                br.write("delta p: " + this.batchSize + "\n");
                br.write("cutoff: " + this.stabilityCutoff + "\n");
                br.write("counter: " + this.stabilityCounter + "\n");
                br.write("pivots selected: " + pivotsSelected + "\n");
                br.write("percent of graph selected: " + percentSelected + "\n");
                br.write("supsersteps: " + this.getSuperstep() + "\n");
                br.write("cycles: " + this.cycle + "\n");
                br.write("run time: " + time + "\n");
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException("Could not write to file: " + filename);
            } finally {
                br.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Could not open file: " + filename);
        }

    }

    /**
     * Set pivots globally in the pivot aggregator
     *
     * @param pivots A collection of selected pivots.
     */
    private void setGlobalPivots(Collection<String> pivots) {
        this.setAggregatedValue(PIVOT_AGG, new PivotSetWritable(pivots));
    }


    /**
     * Write the high betweenness set to a file in hdfs
     *
     * @param set A set of vertices that contain the highest highbetweenness value.
     */
    private void writeHighBetweennessSet(Set<String> set) {
        String defaultFS = getDefaultFS(getConf());
        String filename = defaultFS + "/" + outputDir + "/" + FINAL_SET_CSV;
        Path pt = new Path(filename);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
            try {
                for (String id : set) {
                    br.write(id + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException("Could not write to file: " + filename);
            } finally {
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Could not open file: " + filename);
        }

    }


    /**
     * Set the value of the state aggregator
     *
     * @param state The current state of computation.
     */
    private void setGlobalState(State state) {
        this.setAggregatedValue(STATE_AGG, new IntWritable(state.ordinal()));
    }


    /**
     * Find the number of changes in the high betweenness set.
     * Compares the previous run to the newest computation.
     *
     * @param incomingSet A new set of high betweenness calculations
     * @return the number of changes in the high betweenness set.
     */
    private int compareHighBetweennessSet(Set<String> incomingSet) {
        int diff = 0;
        for (String id : incomingSet) {
            if (!this.highBetweennessSet.contains(id)) {
                diff++;
            }
        }
        return diff;
    }

    /**
     * Get the default file system. used to create a valid hdfs path
     *
     * @param conf Configuration for the current job.
     * @return the default hdfs file system.
     */
    private String getDefaultFS(Configuration conf) {
        return (conf.get(FS_DEFAULT_FS) != null ? conf.get(FS_DEFAULT_FS) : conf.get(FS_DEFAULT_NAME));
    }

    /**
     * Gets the current Pivots that are being computed.
     *
     * @return A set of aggregated pivots.
     */
    private PivotSetWritable getCurrentPivots() {
        return getAggregatedValue(PIVOT_AGG);
    }

    /**
     * Gets all previous pivots used.
     *
     * @return A set of aggregated pivots.
     */
    private PivotSetWritable getPreviousPivots() {
        return getAggregatedValue(PREVIOUS_PIVOT_AGG);
    }

}
