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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;


/**
 * Coordinates Global state for the SBVertex to calculate shortest paths, accumulates pair dependency information,
 * and monitor high betweenness set stability.
 * <p/>
 * Required configuration settings:
 * <p/>
 * * fs.defaultFS OR fs.default.name:  If not set in the environment you can set them as a custom arguments. typically this will not need to be set
 * * betweenness.output.dir: Directory in HDFS used to write the high betweenness set.
 * * betweenness.set.stability: Integer value, algorithm completes with the high betweenness set changes by less than this value, checked after each cycle.
 * * betweenness.set.stability.stabilityRunningCounter: Integer value, number of times the stability threshold must be reached.
 * * betweenness.set.maxSize: Size the result set desired.
 * * pivot.batch.size: Number of pivots to use in each batch
 * * pivot.batch.size.initial:  Number of pivots to use in the first bacth (defaults to pivot.batch.size)
 * * vertex.count: The number of vertices to be loaded
 * * betweenness.shortest.path.phases: Number of shortest path phases to run for every 1 dependency accumulation phase.
 * <p/>
 * WARNING:  id values for all vertices must be in 0,1,2,...N where N=vertex.count-1
 * <p/>
 * For background information on the approximation method see:
 * * "W. Chong, Efficent Extraction of High-Betweenness Vertices"
 * <p/>
 * For background information on the method of accumulation of pair dependencies and shortest path data see:
 * * "U. Brandes, A Faster Algorithm for Betweenness Centrality"
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
     * Configuration Identifier for the set stability stabilityRunningCounter cut off point (margin of error).
     */
    public static final String BETWEENNESS_SET_STABILITY_COUNTER = HBSEMasterCompute.BETWEENNESS_SET_STABILITY + ".counter";
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
    public static final String PIVOT_BATCH_SIZE_INITIAL = HBSEMasterCompute.PIVOT_BATCH_SIZE + ".initial";
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

    /**
     * Stores the number of pivots to use per batch of nodes.
     */
    private int batchSize;
    /**
     * Stores the number of pivots to use for the initial batch of nodes.
     */
    private int initialBatchSize;

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
     * Stores the Id value of the current pivots being used for computation.
     */
    Queue<Integer> currentPivots = new LinkedList<Integer>();

    /**
     * Stores all previous pivots used in other periods of computation.
     */
    Set<Integer> previousPivots = new HashSet<Integer>();

    /**
     * Stores the current set of highbetweenness nodes.
     */
    Set<Integer> highBetweennessSet = new HashSet<Integer>();

    /**
     * Stores the max highbetweenness set size.
     */
    int maxHighBCSetSize;

    /**
     * Current cycle: a cycle is defined here as a shortest path phase + a pair dependency phase
     */
    int cycle = 1;

    /**
     * Stores the stability cut off value after a full cycle.
     */
    int stabilityCutoff;
    /**
     * Stores the number of times the stability counter must be met before exiting.
     */
    int stabilityCounter;
    /**
     * Stores the running count of the number of times the stability counter was met.
     */
    int stabilityRunningCounter = 0;

    /**
     * Output directory in HDFS.
     */
    String outputDir;


    /**
     * Global States that direct certain computation.
     */
    public enum State {
        START,
        SHORTEST_PATH_START,
        SHORTEST_PATH_RUN,
        PAIR_DEPENDENCY_PING_PREDECESSOR,
        PAIR_DEPENDENCY_FIND_SUCCESSORS,
        PAIR_DEPENDENCY_RUN,
        PAIR_DEPENDENCY_COMPLETE,
        FINISHED
    }

    /**
     * Variable that tracks the current state of computation.
     */
    private State state = State.START;

    /**
     * The random variable for choosing new pivot points.
     */
    private Random random;

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
     */
    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        start = new Date();
        state = State.START;
        this.registerPersistentAggregator(STATE_AGG, IntOverwriteAggregator.class);
        this.registerPersistentAggregator(PIVOT_AGG, IntArrayOverwriteAggregator.class);
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

        this.shortestPathPhases = 1;
        this.shortestPathPhasesCompleted = 0;
        String shortestPathPhasesStr = getConf().get(BETWEENNESS_SHORTEST_PATH_PHASES);
        try {
            if (shortestPathPhasesStr != null) shortestPathPhases = Integer.parseInt(shortestPathPhasesStr);
        } catch (NumberFormatException e) {
            LOG.error(HBSEMasterCompute.BETWEENNESS_SHORTEST_PATH_PHASES + " not set to valid int. default=1");
        }


        stabilityCutoff = 0;
        String stabilityCutoffStr = getConf().get(BETWEENNESS_SET_STABILITY);
        if (null != stabilityCutoffStr) {
            try {
                stabilityCutoff = Integer.parseInt(stabilityCutoffStr);
            } catch (NumberFormatException e) {
                LOG.error(HBSEMasterCompute.BETWEENNESS_SET_STABILITY + " must be set to a valid int. default=" + stabilityCutoff);
                throw e;
            }
        }
        LOG.info(BETWEENNESS_SET_STABILITY + "=" + stabilityCutoff);

        stabilityCounter = 3;
        String stabilityCounterStr = getConf().get(BETWEENNESS_SET_STABILITY_COUNTER);
        if (null != stabilityCutoffStr) {
            try {
                stabilityCounter = Integer.parseInt(stabilityCounterStr);
            } catch (NumberFormatException e) {
                LOG.error(HBSEMasterCompute.BETWEENNESS_SET_STABILITY_COUNTER + " must be set to a valid int. default=" + stabilityCounter);
                throw e;
            }
        }
        LOG.info(HBSEMasterCompute.BETWEENNESS_SET_STABILITY_COUNTER + "=" + stabilityCounter);

        maxHighBCSetSize = 1;
        try {
            maxHighBCSetSize = Integer.parseInt(getConf().get(BETWEENNESS_SET_MAX_SIZE));
        } catch (NumberFormatException e) {
            LOG.error(HBSEMasterCompute.BETWEENNESS_SET_MAX_SIZE + " must be set to a valid int.");
            throw e;
        }
        LOG.info(HBSEMasterCompute.BETWEENNESS_SET_MAX_SIZE + "=" + maxHighBCSetSize);


        // manually set first pivot batch if argument is present
        try {
            String pivotBatchStr = this.getConf().get(PIVOT_BATCH_STRING);
            if (null != pivotBatchStr && pivotBatchStr.length() > 0) {
                String[] pivotBatchArray = pivotBatchStr.split(",");
                for (String pivotStr : pivotBatchArray) {
                    int pivot = Integer.parseInt(pivotStr);
                    currentPivots.add(pivot);
                    LOG.info("Manually added pivot: " + pivot);
                }
            }

        } catch (NumberFormatException e) {
            LOG.error("Optional argument " + HBSEMasterCompute.PIVOT_BATCH_STRING + " invalid. Must be a comma seperated list of ints.");
            throw e;
        }
        if (!currentPivots.isEmpty()) setGlobalPivots(currentPivots);


        String batchSizeStr = this.getConf().get(PIVOT_BATCH_SIZE);
        try {
            batchSize = Integer.parseInt(batchSizeStr);
        } catch (NumberFormatException e) {
            LOG.error("Required option not set or invalid. \"" + HBSEMasterCompute.PIVOT_BATCH_SIZE + "\" must be set to a valid int, was set to: " + batchSizeStr);
            throw e;
        }
        LOG.info(HBSEMasterCompute.PIVOT_BATCH_SIZE + "=" + batchSize);

        initialBatchSize = batchSize;
        try {
            String initialBatchSizeStr = getConf().get(PIVOT_BATCH_SIZE_INITIAL);
            if (initialBatchSizeStr != null) initialBatchSize = Integer.parseInt(initialBatchSizeStr);
        } catch (NumberFormatException e) {
            LOG.error("Optional setting " + HBSEMasterCompute.PIVOT_BATCH_SIZE_INITIAL + " set to invalid value, using default");
        }


        String randomSeedStr = getConf().get(PIVOT_BATCH_RANDOM_SEED);
        if (null == randomSeedStr) {
            random = new Random();
        } else {
            long seed = Long.parseLong(randomSeedStr);
            random = new Random(seed);
            LOG.info("Set random seed: " + seed);
        }


        String maxIdStr = this.getConf().get(VERTEX_COUNT);
        try {
            maxId = Integer.parseInt(maxIdStr);
        } catch (NumberFormatException e) {
            LOG.error("Required option not set or invalid. \"" + HBSEMasterCompute.VERTEX_COUNT + "\" must be set to a valid int, was set to: " + maxIdStr);
            throw e;
        }
        LOG.info(HBSEMasterCompute.VERTEX_COUNT + "=" + maxId);

    }


    /**
     * Coordinates the computation phases of SBVertex by monitoring for the completion of each state
     * and moving to the next state.
     * <p/>
     * * selects pivots
     * * monitors for completion of shortest paths
     * * starts pair dependency phase
     * * monitors for completion of pair dependency
     * * checks high betweenness set stability
     * * if set is stable
     * * save set and exit
     * * else
     * * select new pivots and start new shortest path phase
     */
    @Override
    public void compute() {
        long step = this.getSuperstep();
        LOG.info("Superstep: " + step + " starting in State: " + state);

        if (State.START == state) {
            if (currentPivots.isEmpty()) {
                int currentBatchSize = (step == 0) ? this.initialBatchSize : this.batchSize;
                choosePivots(currentBatchSize);
            }
            state = State.SHORTEST_PATH_START;
            setGlobalState(state);
            LOG.info("Superstep: " + step + " Switched to State: " + state);
            return;
        } else if (State.SHORTEST_PATH_START == state) {
            int updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
            LOG.info("Superstep: " + step + " Paths updated: " + updateCount);
            state = State.SHORTEST_PATH_RUN;
            setGlobalState(state);
            LOG.info("Superstep: " + step + " Switched to State: " + state);
        } else if (State.SHORTEST_PATH_RUN == state) {
            int updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
            LOG.info("Superstep: " + step + " Paths updated: " + updateCount);
            if (updateCount == 0) {
                shortestPathPhasesCompleted++;
                if (shortestPathPhasesCompleted == shortestPathPhases) {
                    state = State.PAIR_DEPENDENCY_PING_PREDECESSOR;
                } else {
                    choosePivots(this.batchSize);
                    state = State.SHORTEST_PATH_START;
                }
                setGlobalState(state);
                LOG.info("Superstep: " + step + " UPDATE COUNT 0, shortest path phase " + shortestPathPhasesCompleted + " of " + shortestPathPhases + " Switched to State: " + state);
            }
        } else if (State.PAIR_DEPENDENCY_PING_PREDECESSOR == state) {
            shortestPathPhasesCompleted = 0;
            state = State.PAIR_DEPENDENCY_FIND_SUCCESSORS;
            setGlobalState(state);
            LOG.info("Superstep: " + step + " Switched to State: " + state);
        } else if (State.PAIR_DEPENDENCY_FIND_SUCCESSORS == state) {
            state = State.PAIR_DEPENDENCY_RUN;
            setGlobalState(state);
            LOG.info("Superstep: " + step + " Switched to State: " + state);
        } else if (State.PAIR_DEPENDENCY_RUN == state) {
            int updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
            if (updateCount == 0) {
                state = State.PAIR_DEPENDENCY_COMPLETE;
                setGlobalState(state);
            }
            LOG.info("Superstep: " + step + " UPDATE COUNT " + updateCount + ", State: " + state);
        } else if (State.PAIR_DEPENDENCY_COMPLETE == state) {
            HighBetweennessList hbl = getAggregatedValue(HIGH_BC_SET_AGG);
            Set<Integer> incomingSet = hbl.getHighBetweennessSet();
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
                    choosePivots(this.batchSize);
                    state = State.SHORTEST_PATH_START;
                }

            } else if (currentPivots.size() + previousPivots.size() == this.maxId) {
                LOG.info(logprefix + " All possible pivots selected, exiting");
                state = State.FINISHED;
            } else {
                stabilityRunningCounter = 0; // reset stabilityRunningCounter
                LOG.info(logprefix + " Delta did not meet cutoff, starting next cycle.");
                choosePivots(this.batchSize);
                state = State.SHORTEST_PATH_START;
            }
            setGlobalState(state);
            LOG.info("Superstep: " + step + ", going to State: " + state);
        } else if (State.FINISHED == state) {
            this.haltComputation();
            end = new Date();
            this.writeHighBetweennessSet(highBetweennessSet);
            this.writeStats();
        } else {
            LOG.error("INVALID STATE: " + state);
            throw new IllegalStateException("Invalid State" + state);
        }

    }


    /**
     * Populate currentPivots with a new batch of pivots. Set the value globally with an aggregator
     */
    private void choosePivots(int currentBatchSize) {
        LOG.info("Selecting new pivots.");
        previousPivots.addAll(currentPivots);
        currentPivots.clear();

        StringBuilder b = new StringBuilder();
        b.append("[");

        int attempt = 0;
        while (currentPivots.size() < currentBatchSize && (previousPivots.size() + currentPivots.size()) < maxId) {
            attempt++;
            int pivot = random.nextInt(maxId);
            if (!previousPivots.contains(pivot)) {
                currentPivots.add(pivot);
                b.append(pivot).append(",");
            }
        }
        b.deleteCharAt(b.length() - 1);
        b.append("]");
        LOG.info("Pivot selection complete. Took " + attempt + " attempts.");
        LOG.info("Pivot set: " + b.toString());
        setGlobalPivots(currentPivots);
    }


    private void writeStats() {

        int pivotsSelected = this.currentPivots.size() + this.previousPivots.size();
        double percentSelected = (double) pivotsSelected / this.maxId;
        int time = (int) ((end.getTime() - start.getTime()) / 1000);

        String defaultFS = getDefaultFS(getConf());
        String filename = defaultFS + "/" + outputDir + "/" + STATS_CSV;
        Path pt = new Path(filename);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
            br.write("k: " + this.highBetweennessSet.size() + "\n");
            br.write("delta p: " + this.batchSize + "\n");
            br.write("cutoff: " + this.stabilityCutoff + "\n");
            br.write("counter: " + this.stabilityCounter + "\n");
            br.write("pivots selected: " + pivotsSelected + "\n");
            br.write("percent of graph selected: " + percentSelected + "\n");
            br.write("supsersteps: " + this.getSuperstep() + "\n");
            br.write("cycles: " + this.cycle + "\n");
            br.write("run time: " + time + "\n");


            br.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Could not write to file: " + filename);
        }
    }

    /**
     * Set pivots globally in the pivot aggregator
     *
     * @param pivots
     */
    private void setGlobalPivots(Collection<Integer> pivots) {
        IntWritable[] batch = new IntWritable[pivots.size()];
        int i = 0;
        for (int pivot : pivots) {
            batch[i++] = new IntWritable(pivot);
        }
        this.setAggregatedValue(PIVOT_AGG, new IntArrayWritable(batch));
    }


    /**
     * Write the high betweenness set to a file in hdfs
     *
     * @param set
     */
    private void writeHighBetweennessSet(Set<Integer> set) {
        String defaultFS = getDefaultFS(getConf());
        String filename = defaultFS + "/" + outputDir + "/" + FINAL_SET_CSV;
        Path pt = new Path(filename);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
            for (int id : set) {
                br.write(id + "\n");
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Could not write to file: " + filename);
        }

    }


    /**
     * Set the value of the state aggregator
     *
     * @param state
     */
    private void setGlobalState(State state) {
        this.setAggregatedValue(STATE_AGG, new IntWritable(state.ordinal()));
    }


    /**
     * Find the number of changes in the high betweenness set.
     *
     * @param incomingSet
     * @return the number of changes in the high betweenness set.
     */
    private int compareHighBetweennessSet(Set<Integer> incomingSet) {
        int diff = 0;
        for (int id : incomingSet) {
            if (!this.highBetweennessSet.contains(id)) {
                diff++;
            }
        }
        return diff;
    }


    /**
     * Get the default file system. used to create a valid hdfs path
     *
     * @param conf
     * @return
     */
    private String getDefaultFS(Configuration conf) {
        return (conf.get(FS_DEFAULT_FS) != null ? conf.get(FS_DEFAULT_FS) : conf.get(FS_DEFAULT_NAME));
    }


}
