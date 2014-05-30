package com.soteradefense.dga;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DGACommandLineUtil {

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
        options.addOption("w", true, "The number of giraph workers to use for the analytic");
        options.addOption("ca", true, "Any custom arguments to pass in to giraph");
        options.addOption("yh", true, "Heap size, in MB, task (YARN only.) Defaults to giraph.yarn.task.heap.mb => 1024 (integer) MB.");
        options.addOption("yj", true, "comma-separated list of JAR filenames to distribute to Giraph tasks and ApplicationMaster. YARN only. Search order: CLASSPATH, HADOOP_HOME, user current dir.");

        return options;
    }

    public static DGAConfiguration parseCommandLine(String [] args, Options options) throws ParseException {
        CommandLineParser parser = new BasicParser();
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
            String [] customArguments = cmd.getOptionValues("ca");
            for (String customArgument : customArguments) {
                int indexOfEquals = customArgument.indexOf("=");
                if (indexOfEquals == -1)
                    throw new ParseException("The custom argument " + customArgument + " does not follow the form -ca key=value");
                String key = customArgument.substring(0, indexOfEquals);
                String value = customArgument.substring(indexOfEquals + 1);
                dgaConf.setCustomProperty(key, value);
            }
        }

        return dgaConf;
    }

}
