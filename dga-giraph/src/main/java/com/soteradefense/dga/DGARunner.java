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


import com.soteradefense.dga.hbse.HBSEComputation;
import com.soteradefense.dga.hbse.HBSEConfigurationConstants;
import com.soteradefense.dga.hbse.HBSEMasterCompute;
import com.soteradefense.dga.io.formats.*;
import com.soteradefense.dga.lc.LeafCompressionComputation;
import com.soteradefense.dga.pr.PageRankComputation;
import com.soteradefense.dga.pr.PageRankMasterCompute;
import com.soteradefense.dga.wcc.WeaklyConnectedComponentComputation;
import org.apache.commons.cli.Options;
import org.apache.giraph.GiraphRunner;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Set;
import java.util.TreeSet;

public class DGARunner {

    private static final Logger logger = LoggerFactory.getLogger(DGARunner.class);

    private static final Set<String> supportedAnalytics = new TreeSet<String>();

    static {
        supportedAnalytics.add("louvain");
        supportedAnalytics.add("wcc");
        supportedAnalytics.add("hbse");
        supportedAnalytics.add("lc");
        supportedAnalytics.add("pr");
    }

    public void run(String[] args) throws Exception {
        Options options = DGACommandLineUtil.generateOptions();
        if (args.length < 4)
            DGACommandLineUtil.printUsageAndExit(options);

        String libDir = args[0];
        String analytic = args[1].toLowerCase();
        String inputPath = args[2];
        String outputPath = args[3];

        if (!supportedAnalytics.contains(analytic)) {
            DGACommandLineUtil.printUsageAndExit(options);
        }

        String[] subsetArguments = new String[args.length - 4];
        for (int i = 0; i < subsetArguments.length; i++) {
            subsetArguments[i] = args[i + 4];
        }
        DGAConfiguration commandLineConf = DGACommandLineUtil.parseCommandLine(subsetArguments, options);
        String logLevel = commandLineConf.getCustomArgumentProperties().get(DGALoggingUtil.DGA_LOG_LEVEL);
        DGALoggingUtil.setDGALogLevel(logLevel);

        try {
            InputStream configurationIS = ClassLoader.getSystemClassLoader().getResourceAsStream("dga-config.xml");

            DGAConfiguration fileConf = DGAXMLConfigurationParser.parse(configurationIS);

            if (analytic.equals("wcc")) {
                logger.info("Analytic: Weakly Connected Components");
                DGAConfiguration requiredConf = new DGAConfiguration();
                requiredConf.setDGAGiraphProperty("-eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eof", DGAEdgeTTTOutputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eip", inputPath);
                requiredConf.setDGAGiraphProperty("-op", outputPath);
                requiredConf.setDGAGiraphProperty("-esd", outputPath);
                DGAConfiguration minimalDefaults = new DGAConfiguration();
                minimalDefaults.setCustomProperty(DGAEdgeTTTOutputFormat.WRITE_VERTEX_VALUE, "true");
                DGAConfiguration finalConf = DGAConfiguration.coalesce(minimalDefaults, fileConf, commandLineConf, requiredConf);

                finalConf.setLibDir(libDir);

                String[] giraphArgs = finalConf.convertToCommandLineArguments(WeaklyConnectedComponentComputation.class.getCanonicalName());
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));

            } else if (analytic.equals("hbse")) {
                logger.info("Analytic: High Betweenness Set Extraction");
                DGAConfiguration requiredConf = new DGAConfiguration();
                requiredConf.setDGAGiraphProperty("-eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-vof", HBSEOutputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eip", inputPath);
                requiredConf.setDGAGiraphProperty("-mc", HBSEMasterCompute.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-op", outputPath);
                requiredConf.setDGAGiraphProperty("-vsd", outputPath);
                DGAConfiguration minimalDefaults = new DGAConfiguration();
                minimalDefaults.setCustomProperty(HBSEConfigurationConstants.BETWEENNESS_SET_MAX_SIZE, "10");
                minimalDefaults.setCustomProperty(HBSEConfigurationConstants.BETWEENNESS_OUTPUT_DIR, outputPath);
                minimalDefaults.setCustomProperty(HBSEConfigurationConstants.PIVOT_BATCH_SIZE, "10");
                minimalDefaults.setCustomProperty(HBSEConfigurationConstants.PIVOT_BATCH_SIZE_INITIAL, "10");
                minimalDefaults.setCustomProperty(HBSEConfigurationConstants.TOTAL_PIVOT_COUNT, "5");
                DGAConfiguration finalConf = DGAConfiguration.coalesce(minimalDefaults, fileConf, commandLineConf, requiredConf);

                finalConf.setLibDir(libDir);

                String[] giraphArgs = finalConf.convertToCommandLineArguments(HBSEComputation.class.getCanonicalName());
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));
            } else if (analytic.equals("louvain")) {
                logger.info("Analytic: Louvain Modularity");
                DGAConfiguration partialConfiguration = DGAConfiguration.coalesce(fileConf, commandLineConf);
                partialConfiguration.setDGAGiraphProperty("-eip", inputPath);
                partialConfiguration.setDGAGiraphProperty("-op", outputPath);

                partialConfiguration.setLibDir(libDir);

                LouvainRunner louvainRunner = new LouvainRunner();
                System.exit(louvainRunner.runUntilComplete(inputPath, outputPath, partialConfiguration));
            } else if (analytic.equals("lc")) {
                logger.info("Analytic: Leaf Compression");
                DGAConfiguration requiredConf = new DGAConfiguration();
                requiredConf.setDGAGiraphProperty("-eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eof", DGAEdgeTTTOutputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eip", inputPath);
                requiredConf.setDGAGiraphProperty("-op", outputPath);
                requiredConf.setDGAGiraphProperty("-esd", outputPath);
                DGAConfiguration finalConf = DGAConfiguration.coalesce(fileConf, commandLineConf, requiredConf);

                finalConf.setLibDir(libDir);

                String[] giraphArgs = finalConf.convertToCommandLineArguments(LeafCompressionComputation.class.getCanonicalName());
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));
            } else if (analytic.equals("pr")) {
                logger.info("Analytic: PageRank");
                DGAConfiguration requiredConf = new DGAConfiguration();
                requiredConf.setDGAGiraphProperty("-eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-vof", DGAVertexOutputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eip", inputPath);
                requiredConf.setDGAGiraphProperty("-mc", PageRankMasterCompute.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-op", outputPath);
                requiredConf.setDGAGiraphProperty("-vsd", outputPath);
                requiredConf.setCustomProperty(DGAEdgeTDTOutputFormat.WRITE_VERTEX_VALUE, "true");
                DGAConfiguration finalConf = DGAConfiguration.coalesce(fileConf, commandLineConf, requiredConf);

                finalConf.setLibDir(libDir);

                String[] giraphArgs = finalConf.convertToCommandLineArguments(PageRankComputation.class.getCanonicalName());
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));
            }

        } catch (Exception e) {
            logger.error("Unable to run analytic; ", e);
        }
    }

    public static void main(String[] args) throws Exception {
        DGARunner runner = new DGARunner();
        runner.run(args);
    }

}
