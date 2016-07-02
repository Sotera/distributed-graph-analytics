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

package com.soteradefense.dga;

import com.soteradefense.dga.io.formats.DGALongEdgeValueInputFormat;
import com.soteradefense.dga.io.formats.LouvainVertexInputFormat;
import com.soteradefense.dga.io.formats.LouvainVertexOutputFormat;
import com.soteradefense.dga.louvain.giraph.LouvainComputation;
import com.soteradefense.dga.louvain.giraph.LouvainMasterCompute;
import com.soteradefense.dga.louvain.giraph.LouvainVertexWritable;
import com.soteradefense.dga.louvain.mapreduce.CommunityCompression;
import com.soteradefense.dga.louvain.mapreduce.LouvainTableSynthesizer;
import org.apache.giraph.GiraphRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class LouvainRunner {

    private static Logger logger = LoggerFactory.getLogger(LouvainRunner.class);

    private DGAConfiguration requiredConfiguration;
    private DGAConfiguration minimalDefaultConfiguration;
    private Configuration configuration;

    public LouvainRunner() {
        requiredConfiguration = new DGAConfiguration();
        requiredConfiguration.setDGAGiraphProperty("-vof", LouvainVertexOutputFormat.class.getCanonicalName());
        requiredConfiguration.setDGAGiraphProperty("-mc", LouvainMasterCompute.class.getCanonicalName());

        minimalDefaultConfiguration = new DGAConfiguration();
        minimalDefaultConfiguration.setSystemProperty("giraph.useSuperstepCounters", "false");
        minimalDefaultConfiguration.setCustomProperty("actual.Q.aggregators", "1");
        minimalDefaultConfiguration.setCustomProperty("minimum.progress", "0");
        minimalDefaultConfiguration.setCustomProperty("progress.tries", "1");
        configuration = new Configuration();
    }

    private boolean isComplete(String path) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        Path completeFile = new Path(path + "_COMPLETE");
        return fs.isFile(completeFile);
    }

    public int runUntilComplete(String inputPath, String outputPath, DGAConfiguration partiallyCoalescedConfiguration) throws Exception {
        int status = 0;
        Path output = new Path(outputPath);
        FileSystem fs = FileSystem.get(configuration);
        String basePath = outputPath;
        if (fs.exists(output)) {
            throw new IOException("Output path " + outputPath + " already exists.  Aborting.");
        }

        int iteration = 0;
        outputPath = outputPath.endsWith("/") ? outputPath : outputPath + "/";
        String interimInputPath = inputPath;
        while (!isComplete(outputPath)) {
            String interimOutputPath = outputPath + "giraph_" + String.valueOf(iteration);
            DGAConfiguration confForStep = DGAConfiguration.coalesce(minimalDefaultConfiguration, partiallyCoalescedConfiguration, requiredConfiguration);
            confForStep.setDGAGiraphProperty("-op", interimOutputPath);
            String[] dgaArguments;
            if (iteration == 0) {
                confForStep.setDGAGiraphProperty("-eif", DGALongEdgeValueInputFormat.class.getCanonicalName());
                confForStep.setDGAGiraphProperty("-eip", interimInputPath);
                confForStep.setDGAGiraphProperty("-esd", interimOutputPath);
                confForStep.convertToCommandLineArguments(LouvainComputation.class.getCanonicalName());

                logger.debug("Running Giraph step {} with configuration: {}", iteration, confForStep);

                dgaArguments = confForStep.convertToCommandLineArguments(LouvainComputation.class.getCanonicalName());
            } else {
                confForStep.setDGAGiraphProperty("-vif", LouvainVertexInputFormat.class.getCanonicalName());
                confForStep.setDGAGiraphProperty("-vip", interimInputPath);
                confForStep.setDGAGiraphProperty("-vsd", interimOutputPath);

                logger.debug("Running Giraph step {} with configuration: {}", iteration, confForStep);
                dgaArguments = confForStep.convertToCommandLineArguments(LouvainComputation.class.getCanonicalName());
            }

            System.out.println("dgaArguments: ");
            for (String arg: dgaArguments)
                System.out.print(arg + " ");
            System.out.println();

            status = ToolRunner.run(new GiraphRunner(), dgaArguments);
            if (status != 0)
                return status;

            interimInputPath = interimOutputPath;
            interimOutputPath = outputPath + "mapreduce_" + String.valueOf(iteration);

            status = ToolRunner.run(new CommunityCompression(interimInputPath, interimOutputPath, confForStep), confForStep.buildLibJarsFromLibPath());
            if (status != 0)
                return status;

            interimInputPath = interimOutputPath;
            iteration++;

        }
        requiredConfiguration.setSystemProperty("mapred.reduce.slowstart.completed.maps", "1.0");
        minimalDefaultConfiguration.setSystemProperty("mapred.task.timeout", "7200000");
        DGAConfiguration confForFinalStep = DGAConfiguration.coalesce(minimalDefaultConfiguration, partiallyCoalescedConfiguration, requiredConfiguration);
        return ToolRunner.run(new LouvainTableSynthesizer(basePath, confForFinalStep), confForFinalStep.buildLibJarsFromLibPath());
    }

    private int runMapreduceJob(String inputPath, String outputPath, DGAConfiguration conf) throws Exception {
        Configuration mrConf = new Configuration();
        for (Map.Entry<String, String> entry : conf.getSystemProperties().entrySet()) {
            mrConf.set(entry.getKey(), entry.getValue());
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(LouvainRunner.class);
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

}
