package com.soteradefense.dga.scan1;

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;

/**
 * Created by ekimbrel on 8/20/15.
 */
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

/**
 *  Scan1 determines the neighborhood size of each vertex. And reports the max value found.
 *  Neighborhood size here is defined as the number of edges in the local sub-graph made up of a node and its neighboring vertices.
 *
 *  The master compute class simple collects the aggreated max value and writes to an empty with
 *  the name _vertex_vertexId_value_maxNeighborhoodSize
 *  An empty file with data in the filename is used to avoid consuming an entire block in hdfs for a few bytes of storage.
 *  
 */
public class Scan1MasterCompute extends DefaultMasterCompute {


    private static final Logger logger = LoggerFactory.getLogger(Scan1MasterCompute.class);

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        DGALoggingUtil.setDGALogLevel(this.getConf());
        registerAggregator(Scan1Computation.MAX_AGG, MaxScanAggregator.class);
    }


    @Override
    public void compute() {
        long step = this.getSuperstep();
        if (step == 2) {
            int[] maxScanValue =  (int[]) ((ArrayPrimitiveWritable) this.getAggregatedValue(Scan1Computation.MAX_AGG)).get();
            logger.info("max scan: {}", step, Arrays.toString(maxScanValue));

            String dir =  getConf().get("mapred.output.dir", getConf().get("mapreduce.output.fileoutputformat.outputdir"));
            String path = dir + "/_vertex_"+maxScanValue[0]+"_value_"+maxScanValue[1];
            Path pt = new Path(path);

            try {
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException("Could not write to file: " + path);
            }finally {
                this.haltComputation();
            }
        }
    }

}
