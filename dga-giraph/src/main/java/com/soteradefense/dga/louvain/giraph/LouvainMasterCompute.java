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
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


/**
 * Master compute class. performs a compute function before each super step. Performs 4 functions.
 * <p/>
 * 1.  prints to its standard out the number of nodes that have changed community in each pass
 * 2.  prints to its standard out the Q value of the graph when this phase is complete.
 * 3.  halts the computation when no further progress is being made
 * (each vertex makes the same decision to halt independently on the previous step, and then aggregate their q values)
 * 4.  Determines if this should be the final phase of computation in the pipeline, if so writes a file to indicate such.
 */
public class LouvainMasterCompute  extends DefaultMasterCompute {

    private static final Logger logger = LoggerFactory.getLogger(LouvainMasterCompute.class);

    // track the number of nodes that changed community at each iteration.
    List<Long> history = new ArrayList<Long>();

    // halt on next super step
    boolean halt = false;

    double previousQ;


    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        this.registerAggregator(LouvainComputation.CHANGE_AGG, LongSumAggregator.class);
        this.registerPersistentAggregator(LouvainComputation.TOTAL_EDGE_WEIGHT_AGG, LongSumAggregator.class);
        this.registerPersistentAggregator(LouvainComputation.ACTUAL_Q_AGG, DoubleSumAggregator.class);
        DGALoggingUtil.setDGALogLevel(this.getConf());
    }


    @Override
    public void compute() {

        long currentSuperstep = getSuperstep();
        int currentMinorstep = (int) (currentSuperstep % 3);
        int currentIteration = (int) (currentSuperstep / 3);

        logger.info("currentSuperstep: " + currentSuperstep + " currentMinorstep: " + currentMinorstep + " currentIteration: " + currentIteration); 

        if (currentSuperstep == 0) {
            previousQ = this.getPreviousQvalue();
            logger.info("Previous Q value: {}", previousQ);
        }

        if (currentSuperstep == 1) {
            long m = ((LongWritable) getAggregatedValue(LouvainComputation.TOTAL_EDGE_WEIGHT_AGG)).get();
            logger.info("Graph Weight = {}", m);
        } else if (currentMinorstep == 1 && currentIteration > 0 && currentIteration % 2 == 0) {
            long totalChange = ((LongWritable) getAggregatedValue(LouvainComputation.CHANGE_AGG)).get();
            history.add(totalChange);
            halt = decideToHalt(history, getConf());
            if (halt) {
                logger.info("superstep: {} decided to halt.", currentSuperstep);
            }
            logger.info("superstep: {} pass: {} totalChange: {}", currentSuperstep, (currentIteration / 2), totalChange);

        } else if (halt) {
            double actualQ = getActualQ();
            logger.info("superstep: {} ACTUAL Q: {}", currentSuperstep, actualQ);
            this.haltComputation();

            writeQvalue(Double.toString(actualQ));
            int clippedQ = (int) (actualQ * 10000);
            int clippedPreviousQ = (int) (previousQ * 10000);
            if (currentSuperstep <= 14 || clippedQ <= clippedPreviousQ) {
                markPipeLineComplete(Double.toString(actualQ));
            }
        }

    }

    private double getActualQ() {
        double actualQ = 0.0;
        actualQ += ((DoubleWritable) getAggregatedValue(LouvainComputation.ACTUAL_Q_AGG)).get();
        return actualQ;
    }

    /**
     * Determine if progress is still being made or if the
     * computation should halt.
     *
     * @param history
     * @return
     */
    protected static boolean decideToHalt(List<Long> history, Configuration conf) {
        int minProgress = conf.getInt("minimum.progress", 0);
        int tries = conf.getInt("progress.tries", 1);


        // Halt if the most recent change was 0
        if (0 == history.get(history.size() - 1)) {
            return true;
        }

        //Halt if the change count has increased 4 times
        long previous = history.get(0);
        int count = 0;
        for (long current : history) {
            if (current >= previous - minProgress) {
                count++;
            }
            previous = current;
        }
        return (count > tries);
    }


    /**
     * Saves a file in the hdfs output dir to make that computation is complete.
     * Writes final q value to the file.
     *
     * @param message
     */
    private void markPipeLineComplete(String message) {
        String outputPath = getConf().get("mapred.output.dir", getConf().get("mapreduce.output.fileoutputformat.outputdir"));
        String dir = outputPath.substring(0, outputPath.lastIndexOf("/"));
        String filename = dir + "/_COMPLETE";
//        String filename = getConf().get("fs.defaultFS") + dir + "/_COMPLETE";
        logger.debug("Writing {}", filename);
        writeFile(filename, message);
    }


    private void writeQvalue(String message) {
        String outputPath = getConf().get("mapred.output.dir", getConf().get("mapreduce.output.fileoutputformat.outputdir"));
        int lastIndexOfSlash = outputPath.lastIndexOf("/");
        String dir = outputPath.substring(0, lastIndexOfSlash);
        String stage = outputPath.substring(lastIndexOfSlash + 1);
        String stagenumber = stage.substring(stage.lastIndexOf("_") + 1);
        String filename = dir + "/_q_" + stagenumber;
        writeFile(filename, message);

    }


    private double getPreviousQvalue() {
        String outputPath = getConf().get("mapred.output.dir", getConf().get("mapreduce.output.fileoutputformat.outputdir"));
        int lastIndexOfSlash = outputPath.lastIndexOf("/");
        String dir = outputPath.substring(0, lastIndexOfSlash);
        String stage = outputPath.substring(lastIndexOfSlash + 1);
        String stagenumber = stage.substring(stage.lastIndexOf("_") + 1);
        int previousStageNumber = Integer.parseInt(stagenumber) - 1;
        if (previousStageNumber < 1) {
            return 0.0;
        } else {
            String filename = dir + "/_q_" + previousStageNumber;
            String result = this.readFile(filename).trim();
            return Double.parseDouble(result);
        }
    }


    private void writeFile(String path, String message) {
        Path pt = new Path(path);
        logger.debug("Writing file out to {}, message {}", path, message);
        try {
            //FileSystem fs = FileSystem.get(new Configuration());
            FileSystem fs = FileSystem.get(getConf());
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
            br.write(message);
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Could not write to file: " + path);
        }
    }


    private String readFile(String path) {
        StringBuilder builder = new StringBuilder();
        try {
            Path pt = new Path(path);
            //FileSystem fs = FileSystem.get(new Configuration());
            FileSystem fs = FileSystem.get(getConf());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();
            while (line != null) {
                builder.append(line);
                line = br.readLine();
            }
        } catch (Exception e) {
            throw new IllegalStateException(" Could not read file: " + path);
        }
        return builder.toString();
    }


}



