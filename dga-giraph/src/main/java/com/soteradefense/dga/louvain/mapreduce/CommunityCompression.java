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

package com.soteradefense.dga.louvain.mapreduce;

import com.soteradefense.dga.DGAConfiguration;
import com.soteradefense.dga.DGALoggingUtil;
import com.soteradefense.dga.louvain.giraph.LouvainVertexWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;


/**
 * Map reduce job to compress a graph in such a way that each community is represented by a single node.
 * <p/>
 * input format:  see LouvainVertexOutputFormat
 * output format  see LouvainVertexInputFormat
 * *** the input to this job is output of the BSP computation, the output of this job is the input to the next stage of BSP.
 */
public class CommunityCompression extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(CommunityCompression.class);

    private String inputPath;
    private String outputPath;
    private DGAConfiguration dgaConfiguration;

    public CommunityCompression(String inputPath, String outputPath, DGAConfiguration dgaConfiguration) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.dgaConfiguration = dgaConfiguration;
    }

    public int run(String[] args) throws Exception {
        Configuration mrConf = this.getConf();
        for (java.util.Map.Entry<String, String> entry : dgaConfiguration.getSystemProperties().entrySet()) {
            mrConf.set(entry.getKey(), entry.getValue());
        }

        Job job = Job.getInstance(mrConf);
        job.setJarByClass(CommunityCompression.class);
        Path in = new Path(inputPath);
        Path out = new Path(outputPath);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJobName("CommunityCompression");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LouvainVertexWritable.class);

        job.setMapperClass(CommunityCompression.Map.class);
        job.setReducerClass(CommunityCompression.Reduce.class);

        logger.debug("Running Mapreduce step with job configuration: {}", job);

        return job.waitForCompletion(false) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, LouvainVertexWritable> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            DGALoggingUtil.setDGALogLevel(context.getConfiguration());
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().trim().split("\t");
            if (3 > tokens.length) {
                throw new IllegalArgumentException("Expected 4 cols: got " + tokens.length + "  from line: " + tokens.toString());
            }

            Text outKey = new Text(tokens[1]); // group by community
            String edgeListStr = (tokens.length == 3) ? "" : tokens[3];
            LouvainVertexWritable outValue = LouvainVertexWritable.fromTokens(tokens[2], edgeListStr);
            context.write(outKey, outValue);
        }
    }


    public static class Reduce extends Reducer<Text, LouvainVertexWritable, Text, Text> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            DGALoggingUtil.setDGALogLevel(context.getConfiguration());
        }

        @Override
        public void reduce(Text key, Iterable<LouvainVertexWritable> values, Context context) throws IOException, InterruptedException {
            String communityId = key.toString();
            long weight = 0;
            HashMap<String, Long> edgeMap = new HashMap<String, Long>();
            for (LouvainVertexWritable vertex : values) {
                weight += vertex.getWeight();
                for (Entry<String, Long> entry : vertex.getEdges().entrySet()) {
                    String entryKey = entry.getKey();

                    if (entryKey.equals(communityId)) {
                        weight += entry.getValue();
                    } else if (edgeMap.containsKey(entryKey)) {
                        long w = edgeMap.get(entryKey) + entry.getValue();
                        edgeMap.put(entryKey, w);
                    } else {
                        edgeMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }

            StringBuilder b = new StringBuilder();
            b.append(weight).append("\t");
            for (Entry<String, Long> entry : edgeMap.entrySet()) {
                b.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            b.setLength(b.length() - 1);

            context.write(new Text(key.toString()), new Text(b.toString()));

        }
    }

}
