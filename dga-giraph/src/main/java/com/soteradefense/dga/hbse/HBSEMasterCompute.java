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
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;


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
 * See <a href="http://www.google.com/url?url=http://scholar.google.com/scholar_url%3Fhl%3Den%26q%3Dhttp://www.researchgate.net/publication/221273491_Efficient_Extraction_of_High-Betweenness_Vertices/file/3deec52a5dd8a6faa1.pdf%26sa%3DX%26scisig%3DAAGBfm1Xl41dnryyDhAGnt9AYOL6iHLoOg%26oi%3Dscholarr&rct=j&q=&esrc=s&sa=X&ei=j15yU7mwGMKdyATX7ID4BA&ved=0CC0QgAMoADAA&usg=AFQjCNH-dTuG7bYZk4__IQGwGvFrnQ9mGQ&cad=rja">"W. Chong, Efficent Extraction of High-Betweenness Vertices"</a>
 * <p/>
 * For background information on the method of accumulation of pair dependencies and shortest path data see:
 * See <a href="http://www.google.com/url?url=http://scholar.google.com/scholar_url%3Fhl%3Den%26q%3Dhttp://kops.ub.uni-konstanz.de/bitstream/handle/urn:nbn:de:bsz:352-opus-71888/algorithm.pdf%253Fsequence%253D1%26sa%3DX%26scisig%3DAAGBfm2tszb3JWsE0Mp8E5os2p-udyVKtw%26oi%3Dscholarr&rct=j&q=&esrc=s&sa=X&ei=4l5yU7CuOsOPyAS-7YLoBw&ved=0CCgQgAMoADAA&usg=AFQjCNGKQ_j2h7QQSQncFUGgkpKO4Uo3Yw&cad=rja">"U. Brandes, A Faster Algorithm for Betweenness Centrality"</a>
 */
public class HBSEMasterCompute extends DefaultMasterCompute {

    private static final Logger logger = LoggerFactory.getLogger(HBSEMasterCompute.class);

    /**
     * Aggregator Identifier that gets the state of the computation.
     */
    public static final String STATE_AGG = "com.sotera.graph.singbetweenness.STATE_AGG";

    /**
     * Current List Of Pivots Being Used.
     */
    public static final String PIVOT_AGG = "com.sotera.graph.singbetweenness.PIVOT_AGG";

    /**
     * Aggregator Identifier for the number of nodes changed in the highbetweenness list comparison.
     */
    public static final String UPDATE_COUNT_AGG = "com.sotera.graph.singbetweenness.UPDATE_COUNT_AGG";

    /**
     * Aggregator Identifier for the saved highbetweenness set.
     */
    public static final String HIGH_BC_SET_AGG = "com.sotera.graph.singbetweenness.HIGH_BC_SET_AGG";

    /**
     * Aggregator Identifier that stores the number of pivots to choose per batch.
     */
    public static final String PIVOT_COUNT = "pivot.count.cutoff";

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

    private boolean initialBatch;
    private int pivotCount;
    private int initialPivotCount;
    private int totalPivotsSelected;

    /**
     * Read options from configuration file and set up aggregators (global communication)
     *
     * @throws InstantiationException if it is unable to register an aggregator with a specific class.
     * @throws IllegalAccessException if it is unable to access the class passed in as an aggregator.
     */
    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        DGALoggingUtil.setDGALogLevel(this.getConf());
        start = new Date();
        state = State.START;
        this.registerPersistentAggregator(STATE_AGG, IntOverwriteAggregator.class);
        this.registerPersistentAggregator(PIVOT_AGG, PivotListAggregator.class);
        this.registerPersistentAggregator(PIVOT_COUNT, IntOverwriteAggregator.class);
        this.registerAggregator(UPDATE_COUNT_AGG, IntSumAggregator.class);
        this.registerAggregator(HIGH_BC_SET_AGG, HighBetweennessListAggregator.class);
        String defaultFS = this.getDefaultFS(this.getConf());
        if (defaultFS == null) {
            throw new IllegalArgumentException(HBSEConfigurationConstants.FS_DEFAULT_FS + " OR " + HBSEConfigurationConstants.FS_DEFAULT_NAME + " must be set.  If not set in the environment you can set them " +
                    "as a custom argument to the" +
                    " giraph job via -ca fs.default.name=<your default fs>");
        }

        outputDir = getConf().get(HBSEConfigurationConstants.BETWEENNESS_OUTPUT_DIR);
        if (outputDir == null || outputDir.length() < 1) {
            throw new IllegalArgumentException(HBSEConfigurationConstants.BETWEENNESS_OUTPUT_DIR + " must be set to a valid directory in HDFS");
        }

        shortestPathPhases = getOptionalHBSEConfiguration(HBSEConfigurationConstants.BETWEENNESS_SHORTEST_PATH_PHASES, 1);
        this.shortestPathPhasesCompleted = 0;

        stabilityCutoff = getOptionalHBSEConfiguration(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY, 0);
        logger.info(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY + "=" + stabilityCutoff);

        stabilityCounter = getOptionalHBSEConfiguration(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY_COUNTER, 1);
        logger.info(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY_COUNTER + "=" + stabilityCounter);

        int maxHighBCSetSize = getRequiredHBSEConfiguration(HBSEConfigurationConstants.BETWEENNESS_SET_MAX_SIZE);
        logger.info(HBSEConfigurationConstants.BETWEENNESS_SET_MAX_SIZE + "=" + maxHighBCSetSize);

        try {
            pivotCount = Integer.parseInt(getConf().get(HBSEConfigurationConstants.PIVOT_BATCH_SIZE));
        } catch (NumberFormatException e) {
            logger.error("Option not set or invalid. \"" + HBSEConfigurationConstants.PIVOT_BATCH_SIZE + "\" must be set to a valid double, was set to " + getConf().get(HBSEConfigurationConstants.PIVOT_BATCH_SIZE), e);
            throw e;
        }
        logger.info(HBSEConfigurationConstants.PIVOT_BATCH_SIZE + "=" + pivotCount);


        try {
            initialPivotCount = Integer.parseInt(getConf().get(HBSEConfigurationConstants.PIVOT_BATCH_SIZE_INITIAL, getConf().get(HBSEConfigurationConstants.PIVOT_BATCH_SIZE)));
        } catch (NumberFormatException e) {
            logger.error("Option not set or invalid. \"" + HBSEConfigurationConstants.PIVOT_BATCH_SIZE_INITIAL + "\" must be set to a valid double, " +
                    "was set to " + getConf().get(HBSEConfigurationConstants.PIVOT_BATCH_SIZE_INITIAL), e);
            throw e;
        }
        initialBatch = true;
        maxId = getRequiredHBSEConfiguration(HBSEConfigurationConstants.TOTAL_PIVOT_COUNT);
        setAggregatedValue(PIVOT_COUNT, new IntWritable(pivotCount));
        logger.info(HBSEConfigurationConstants.TOTAL_PIVOT_COUNT + "=" + maxId);
        totalPivotsSelected = 0;

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
            logger.error("Option not set or invalid. \"" + name + "\" must be set to a valid int, was set to: " + propValue, e);
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
            logger.error("Option not set or invalid. \"" + name + "\" must be set to a valid int, was set to: " + defaultValue, e);
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
        logger.info("Superstep: " + step + " starting in State: " + state);
        switch (state) {
            case START:
                state = State.PIVOT_SELECTION;
                setGlobalState(state);
                logger.info("Superstep: " + step + " Switched to State: " + state);
                break;
            case PIVOT_SELECTION:
                boolean allPivotsAreSelected;
                if (initialBatch) {
                    allPivotsAreSelected = areAllPivotsSelected(initialPivotCount);
                    initialBatch = !allPivotsAreSelected;
                } else {
                    allPivotsAreSelected = areAllPivotsSelected(pivotCount);
                }
                if (allPivotsAreSelected) {
                    int pivotCount = ((PivotList) getAggregatedValue(PIVOT_AGG)).getPivots().size();
                    logger.debug("Pivots have been selected!  The count is " + pivotCount);
                    totalPivotsSelected += pivotCount;
                    state = State.SHORTEST_PATH_START;
                    setGlobalState(state);
                }
                break;
            case SHORTEST_PATH_START:
                int updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
                logger.info("Superstep: " + step + " Paths updated: " + updateCount);
                state = State.SHORTEST_PATH_RUN;
                setGlobalState(state);
                logger.info("Superstep: " + step + " Switched to State: " + state);
                break;
            case SHORTEST_PATH_RUN:
                updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
                logger.info("Superstep: " + step + " Paths updated: " + updateCount);
                if (updateCount == 0) {
                    shortestPathPhasesCompleted++;
                    if (shortestPathPhasesCompleted == shortestPathPhases) {
                        state = State.PAIR_DEPENDENCY_PING_PREDECESSOR;
                    } else {
                        state = State.SHORTEST_PATH_START;
                    }
                    setGlobalState(state);
                    logger.info("Superstep: " + step + " UPDATE COUNT 0, shortest path phase " + shortestPathPhasesCompleted + " of " + shortestPathPhases + " Switched to State: " + state);
                }
                break;
            case PAIR_DEPENDENCY_PING_PREDECESSOR:
                shortestPathPhasesCompleted = 0;
                state = State.PAIR_DEPENDENCY_FIND_SUCCESSORS;
                setGlobalState(state);
                logger.info("Superstep: " + step + " Switched to State: " + state);
                break;
            case PAIR_DEPENDENCY_FIND_SUCCESSORS:
                state = State.PAIR_DEPENDENCY_RUN;
                setGlobalState(state);
                logger.info("Superstep: " + step + " Switched to State: " + state);
                break;
            case PAIR_DEPENDENCY_RUN:
                updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
                if (updateCount == 0) {
                    state = State.PAIR_DEPENDENCY_COMPLETE;
                    setGlobalState(state);
                }
                logger.info("Superstep: " + step + " UPDATE COUNT " + updateCount + ", State: " + state);
                break;
            case PAIR_DEPENDENCY_COMPLETE:
                HighBetweennessList hbl = getAggregatedValue(HIGH_BC_SET_AGG);
                Set<String> incomingSet = hbl.getHighBetweennessSet();
                int delta = this.compareHighBetweennessSet(incomingSet);
                highBetweennessSet = incomingSet;

                logger.info("High Betweenness Set Delta: " + delta);
                String logprefix = "Finished Cycle: " + cycle;
                cycle++;

                if (delta <= stabilityCutoff) {
                    stabilityRunningCounter++;
                    if (stabilityRunningCounter >= stabilityCounter) {
                        logger.info(logprefix + " Set Delta < cutoff value; counter=" + stabilityRunningCounter + " approximation complete.");
                        state = State.FINISHED;
                    } else {
                        logger.info(logprefix + " Set Delta < cutoff value; counter=" + stabilityRunningCounter);
                        state = State.SHORTEST_PATH_START;
                    }

                } else if (totalPivotsSelected >= this.maxId) {
                    logger.info(logprefix + " All possible pivots selected, exiting");
                    state = State.FINISHED;
                } else {
                    stabilityRunningCounter = 0; // reset stabilityRunningCounter
                    logger.info(logprefix + " Delta did not meet cutoff, starting next cycle.");
                    state = State.SHORTEST_PATH_START;
                }
                setGlobalState(state);
                logger.info("Superstep: " + step + ", going to State: " + state);
                break;
            case FINISHED:
                this.haltComputation();
                end = new Date();
                this.writeHighBetweennessSet(highBetweennessSet);
                this.writeStats();
                break;
            default:
                logger.error("INVALID STATE: " + state);
                throw new IllegalStateException("Invalid State" + state);
        }
    }

    /**
     * Writes the various statistics when computation finishes.
     */
    private void writeStats() {
        double percentSelected = (double) totalPivotsSelected / this.maxId;
        int time = (int) ((end.getTime() - start.getTime()) / 1000);

        String defaultFS = getDefaultFS(getConf());
        String filename = defaultFS + "/" + outputDir + "/" + HBSEConfigurationConstants.STATS_CSV;
        Path pt = new Path(filename);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
            try {
                br.write("k: " + this.highBetweennessSet.size() + "\n");
                br.write("delta p: " + this.pivotCount + "\n");
                br.write("cutoff: " + this.stabilityCutoff + "\n");
                br.write("counter: " + this.stabilityCounter + "\n");
                br.write("pivots selected: " + totalPivotsSelected + "\n");
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
     * Write the high betweenness set to a file in hdfs
     *
     * @param set A set of vertices that contain the highest highbetweenness value.
     */
    private void writeHighBetweennessSet(Set<String> set) {
        String defaultFS = getDefaultFS(getConf());
        String filename = defaultFS + "/" + outputDir + "/" + HBSEConfigurationConstants.FINAL_SET_CSV;
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
        return (conf.get(HBSEConfigurationConstants.FS_DEFAULT_FS) != null ? conf.get(HBSEConfigurationConstants.FS_DEFAULT_FS) : conf.get(HBSEConfigurationConstants.FS_DEFAULT_NAME));
    }

    private boolean areAllPivotsSelected(int numberOfPivots) {
        PivotList pivots = getAggregatedValue(PIVOT_AGG);
        if (pivots.getPivots().size() > numberOfPivots) {
            pivots.trim(numberOfPivots);
        }
        return pivots.getPivots().size() == numberOfPivots;
    }
}
