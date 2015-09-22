package com.soteradefense.dga.triangles;

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;

/**
 * Created by ekimbrel on 9/22/15.
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


public class TriangleCountMasterCompute extends DefaultMasterCompute {


    private static final Logger logger = LoggerFactory.getLogger(TriangleCountMasterCompute.class);
    public static final String TRI_COUNT_AGG = "com.soteradefense.dga.triangles.count_aggregator";


    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        DGALoggingUtil.setDGALogLevel(this.getConf());
        registerAggregator(TRI_COUNT_AGG, LongSumAggregator.class);
    }


    @Override
    public void compute() {
        long step = this.getSuperstep();
        if (step == 4L) {
            long totalTriangles = ((LongWritable) getAggregatedValue(TRI_COUNT_AGG)).get();
            double distinctTriangles = totalTriangles / 3.0;

            logger.info("triangles: {}", step, distinctTriangles);

            String dir =  getConf().get("mapred.output.dir", getConf().get("mapreduce.output.fileoutputformat.outputdir"));
            String path = dir + "/_triangles_"+distinctTriangles;
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
