package com.soteradefense.dga;


import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.soteradefense.dga.io.formats.DGATextEdgeValueInputFormat;
import com.soteradefense.dga.io.formats.SimpleEdgeOutputFormat;
import com.soteradefense.dga.leafcompression.LeafCompressionComputation;
import org.apache.giraph.GiraphRunner;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static void printUsageAndExit(int exitCode) {
        System.out.println("hadoop jar dga-giraph-0.0.1.jar com.soteradefense.dga.DGARunner <analytic> <input-path> <output-path>\n");
        System.out.println("\tPermissible values for <analytic> are:");
        System.out.println("\t\twcc - Weakly Connected Components");
        System.out.println("\t\thbse - High Betweenness Set Extraction");
        System.out.println("\t\tlouvain - Louvain Modularity");
        System.out.println("\t\tlc - Leaf Compression");
        System.out.println("\t\tpr - Page Rank\n");
        System.out.println("Both input-path and output-path should be hdfs paths");
        System.exit(exitCode);
    }

    private static int executeLouvain(List<String> giraphArgs, String inputPath, String outputPath) {
        return 1;
    }

    private static int executePR(List<String> giraphArgs, String inputPath, String outputPath) {
        return 1;
    }

    public static void main(String [] args) throws Exception {
        if (args.length == 1 && (args[0].equals("-h") || args[0].equals("--help")))
            printUsageAndExit(0);
        if (args.length < 3)
            printUsageAndExit(1);

        String analytic = args[0].toLowerCase();
        String inputPath = args[1];
        String outputPath = args[2];

        if (!supportedAnalytics.contains(analytic)) {
            printUsageAndExit(1);
        }

        try {

            InputStream configurationIS = ClassLoader.getSystemResourceAsStream("dga-properties.xml");
            Map<String, String> emptyMap = Collections.EMPTY_MAP;

            if (analytic.equals("wcc")) {
                //System.exit(executeWCC(giraphArgs, inputPath, outputPath));
            } else if (analytic.equals("hbse")) {

            } else if (analytic.equals("louvain")) {

            } else if (analytic.equals("lc")) {
                Map<String,String> lcRequiredSettings = new HashMap<String, String>();
                lcRequiredSettings.put("eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                lcRequiredSettings.put("eip", inputPath);
                lcRequiredSettings.put("eof", SimpleEdgeOutputFormat.class.getCanonicalName());
                lcRequiredSettings.put("op", outputPath);
                lcRequiredSettings.put("esd", outputPath);
                String [] giraphArgs = DGAConfigurationUtil.generateDGAArgumentsFromAllSources(LeafCompressionComputation.class.getCanonicalName(), emptyMap, emptyMap, lcRequiredSettings, emptyMap, configurationIS, args);
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));
            } else if (analytic.equals("pr")) {

            }

        } catch (Exception e) {
            logger.error("Unable to run analytic; " , e);
            printUsageAndExit(1);
        }
    }

}
