package com.soteradefense.dga;


import com.soteradefense.dga.io.formats.DGATextEdgeValueInputFormat;
import com.soteradefense.dga.io.formats.SimpleEdgeOutputFormat;
import com.soteradefense.dga.leafcompression.LeafCompressionComputation;
import org.apache.commons.cli.Options;
import org.apache.giraph.GiraphRunner;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;

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

    public static void main(String [] args) throws Exception {

        Options options = DGACommandLineUtil.generateOptions();

        if (args.length < 3)
            DGACommandLineUtil.printUsageAndExit(options);

        String analytic = args[0].toLowerCase();
        String inputPath = args[1];
        String outputPath = args[2];

        if (!supportedAnalytics.contains(analytic)) {
            DGACommandLineUtil.printUsageAndExit(options);
        }

        String [] subsetArguments = new String[args.length - 3];
        for (int i = 0; i < subsetArguments.length; i++) {
            subsetArguments[i] = args[i+3];
        }

        try {
            InputStream configurationIS = ClassLoader.getSystemResourceAsStream("dga-config.xml");

            DGAConfiguration fileConf = DGAXMLConfigurationParser.parse(configurationIS);
            DGAConfiguration commandLineConf = DGACommandLineUtil.parseCommandLine(subsetArguments, options);

            if (analytic.equals("wcc")) {
                //System.exit(executeWCC(giraphArgs, inputPath, outputPath));
            } else if (analytic.equals("hbse")) {

            } else if (analytic.equals("louvain")) {

            } else if (analytic.equals("lc")) {
                DGAConfiguration requiredConf = new DGAConfiguration();
                requiredConf.setDGAGiraphProperty("-eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eof", SimpleEdgeOutputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eip", inputPath);
                requiredConf.setDGAGiraphProperty("-op", outputPath);
                requiredConf.setDGAGiraphProperty("-esd", outputPath);

                DGAConfiguration finalConfiguration = DGAConfiguration.coalesce(fileConf, commandLineConf, requiredConf);

                String [] giraphArgs = finalConfiguration.convertToCommandLineArguments(LeafCompressionComputation.class.getCanonicalName());
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));
            } else if (analytic.equals("pr")) {

            }

        } catch (Exception e) {
            logger.error("Unable to run analytic; " , e);
            System.exit(1);
        }
    }

}
