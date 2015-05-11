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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Map;

public class LouvainTableSynthesizer extends Configured implements Tool {

    public static final String TABLE_BASE_NAME = "table";
    public static final String GIRAPH_FOLDER_BASE_NAME = "giraph";
    public static final String FILE_NAME_SEPARATOR = "_";

    private String basePath;
    private DGAConfiguration dgaConfiguration;

    public LouvainTableSynthesizer(String basePath, DGAConfiguration dgaConfiguration){
        this.basePath = basePath;
        this.dgaConfiguration = dgaConfiguration;
    }


    @Override
	public int run(String[] args) throws Exception {
        Job job = null;
        try {
            int iteration = 0;
            if (!basePath.endsWith("/"))
                basePath = basePath + "/";
            String inputPath = basePath + GIRAPH_FOLDER_BASE_NAME + FILE_NAME_SEPARATOR + iteration;
            String joinPath = basePath + GIRAPH_FOLDER_BASE_NAME + FILE_NAME_SEPARATOR + (iteration + 1);
            String outputPath = basePath + TABLE_BASE_NAME + FILE_NAME_SEPARATOR + iteration;
            Configuration mrConf = this.getConf();
            job = Job.getInstance(mrConf);
            for(Map.Entry<String,String> entry : dgaConfiguration.getSystemProperties().entrySet()){
                mrConf.set(entry.getKey(), entry.getValue());
            }
            FileSystem fs = FileSystem.get(job.getConfiguration());
            boolean nextFileExists = fs.exists(new Path(joinPath));
            while (nextFileExists) {
                System.out.println("Processing " + inputPath + " and " + joinPath);
                job = Job.getInstance(mrConf);
                job.setJobName("Louvain Table Synthesizer " + iteration);

                job.setJarByClass(LouvainTableSynthesizer.class);

                job.setMapperClass(LouvainTableSynthesizerMapper.class);
                job.setReducerClass(LouvainTableSynthesizerReducer.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                //Reducer Output
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);

                //Add both input folders
                Path in = new Path(inputPath);
                Path joinIn = new Path(joinPath);
                Path out = new Path(outputPath);
                FileInputFormat.addInputPath(job, in);
                FileInputFormat.addInputPath(job, joinIn);
                FileOutputFormat.setOutputPath(job, out);

                job.waitForCompletion(true);
                //Set the new temp input path
                inputPath = outputPath;
                iteration++;
                outputPath = basePath + TABLE_BASE_NAME + FILE_NAME_SEPARATOR + iteration;
                joinPath = basePath + GIRAPH_FOLDER_BASE_NAME + FILE_NAME_SEPARATOR + (iteration + 1);
                nextFileExists = fs.exists(new Path(joinPath));
            }

        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return -1;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

}