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

import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;

public class DGACommandLineUtil {

    private static Map<String, Character> characterMap = new HashMap<String, Character>();

    static {
        characterMap.put("\\t", '\t');
        characterMap.put(",", ',');
    }

    public static void printUsageAndExit(Options options) {
        printUsageAndExit(options, 0);
    }

    public static void printUsageAndExit(Options options, int exitCode) {
        String commandLine = "hadoop jar dga-giraph-0.0.1.jar com.soteradefense.dga.DGARunner <analytic> <input-path> <output-path> [options]\n" +
                "\t\n" +
                "\t\tlouvain - Louvain Modularity\n" +
                "\t\thbse - High Betweenness Set Extraction\n" +
                "\t\twcc - Weakly Connected Components\n" +
                "\t\tlc - Leaf Compression\n" +
                "\t\tpr - Page Rank\n";
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(commandLine, options);
        System.exit(exitCode);
    }

    public static Options generateOptions() {
        Options options = new Options();
        options.addOption("h", false, "Prints this help documentation and exits");
        options.addOption("q", false, "Run analytic in quiet mode");
        options.addOption("D", true, "System parameters to pass through to be added to the conf");
        options.addOption("w", true, "The number of giraph workers to use for the analytic");
        options.addOption("ca", true, "Any custom arguments to pass in to giraph");
        options.addOption("yh", true, "Heap size, in MB, task (YARN only.) Defaults to giraph.yarn.task.heap.mb => 1024 (integer) MB.");
        options.addOption("yj", true, "comma-separated list of JAR filenames to distribute to Giraph tasks and ApplicationMaster. YARN only. Search order: CLASSPATH, HADOOP_HOME, user current dir.");
        return options;
    }

    public static DGAConfiguration parseCommandLine(String[] args, Options options) throws ParseException {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h")) {
            printUsageAndExit(options);
        }

        DGAConfiguration dgaConf = new DGAConfiguration();

        if (cmd.hasOption("q")) {
            dgaConf.setGiraphProperty("-q", "");
        }

        if (cmd.hasOption("w")) {
            dgaConf.setGiraphProperty("-w", cmd.getOptionValue("w"));
        }

        if (cmd.hasOption("yj")) {
            dgaConf.setGiraphProperty("-yj", cmd.getOptionValue("yj"));
        }

        if (cmd.hasOption("yh")) {
            dgaConf.setGiraphProperty("-yh", cmd.getOptionValue("yh"));
        }

        if (cmd.hasOption("ca")) {
            String[] customArguments = cmd.getOptionValues("ca");
            for (String customArgument : customArguments) {
                int indexOfEquals = customArgument.indexOf("=");
                if (indexOfEquals == -1)
                    throw new ParseException("The custom argument " + customArgument + " does not follow the form -ca key=value");
                String key = customArgument.substring(0, indexOfEquals);
                String value = customArgument.substring(indexOfEquals + 1);
                if (characterMap.containsKey(value)) {
                    value = String.valueOf(characterMap.get(value));
                }
                dgaConf.setCustomProperty(key, value);
            }
        }

        if (cmd.hasOption("D")) {
            String[] systemProperty = cmd.getOptionValues("D");
            for (String sysProp : systemProperty) {
                int indexOfEquals = sysProp.indexOf("=");
                if (indexOfEquals == -1)
                    throw new ParseException("The systemProperty " + sysProp + " does not follow the form -D key=value");
                String key = sysProp.substring(0, indexOfEquals);
                String value = sysProp.substring(indexOfEquals + 1);
                dgaConf.setSystemProperty(key, value);
            }
        }

        return dgaConf;
    }

}
