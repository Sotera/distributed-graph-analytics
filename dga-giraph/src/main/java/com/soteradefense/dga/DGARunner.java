package com.soteradefense.dga;


import com.soteradefense.dga.hbse.HBSEComputation;
import com.soteradefense.dga.hbse.HBSEMasterCompute;
import com.soteradefense.dga.io.formats.DGATextEdgeValueInputFormat;
import com.soteradefense.dga.io.formats.HBSEOutputFormat;
import com.soteradefense.dga.io.formats.SimpleEdgeOutputFormat;
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

    public static void main(String[] args) throws Exception {

        Options options = DGACommandLineUtil.generateOptions();

        if (args.length < 3)
            DGACommandLineUtil.printUsageAndExit(options);

        String analytic = args[0].toLowerCase();
        String inputPath = args[1];
        String outputPath = args[2];

        if (!supportedAnalytics.contains(analytic)) {
            DGACommandLineUtil.printUsageAndExit(options);
        }

        String[] subsetArguments = new String[args.length - 3];
        for (int i = 0; i < subsetArguments.length; i++) {
            subsetArguments[i] = args[i + 3];
        }
        DGAConfiguration commandLineConf = DGACommandLineUtil.parseCommandLine(subsetArguments, options);
        String logLevel = commandLineConf.getCustomArgumentProperties().get(DGALoggingUtil.DGA_LOG_LEVEL);
        DGALoggingUtil.setDGALogLevel(logLevel);

        try {
            InputStream configurationIS = Thread.currentThread().getContextClassLoader().getResourceAsStream("dga-config.xml");

            DGAConfiguration fileConf = DGAXMLConfigurationParser.parse(configurationIS);

            if (analytic.equals("wcc")) {
                DGAConfiguration requiredConf = new DGAConfiguration();
                requiredConf.setDGAGiraphProperty("-eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eof", SimpleEdgeOutputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eip", inputPath);
                requiredConf.setDGAGiraphProperty("-op", outputPath);
                DGAConfiguration minimalDefaults = new DGAConfiguration();
                minimalDefaults.setCustomProperty(SimpleEdgeOutputFormat.SIMPLE_WRITE_SOURCE_VALUE, "true");
                DGAConfiguration finalConf = DGAConfiguration.coalesce(minimalDefaults, fileConf, commandLineConf, requiredConf);
                String[] giraphArgs = finalConf.convertToCommandLineArguments(WeaklyConnectedComponentComputation.class.getCanonicalName());
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));

            } else if (analytic.equals("hbse")) {
                DGAConfiguration requiredConf = new DGAConfiguration();
                requiredConf.setDGAGiraphProperty("-eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-vof", HBSEOutputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eip", inputPath);
                requiredConf.setDGAGiraphProperty("-mc", HBSEMasterCompute.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-op", outputPath);
                DGAConfiguration minimalDefaults = new DGAConfiguration();
                minimalDefaults.setCustomProperty(HBSEMasterCompute.BETWEENNESS_SET_MAX_SIZE, "10");
                minimalDefaults.setCustomProperty(HBSEMasterCompute.BETWEENNESS_OUTPUT_DIR, outputPath);
                minimalDefaults.setCustomProperty(HBSEMasterCompute.PIVOT_BATCH_SIZE, "50");
                minimalDefaults.setCustomProperty(HBSEMasterCompute.INITIAL_PIVOT_PERCENT, "75");
                minimalDefaults.setCustomProperty(HBSEMasterCompute.VERTEX_COUNT, "5");
                DGAConfiguration finalConf = DGAConfiguration.coalesce(minimalDefaults, fileConf, commandLineConf, requiredConf);
                String[] giraphArgs = finalConf.convertToCommandLineArguments(HBSEComputation.class.getCanonicalName());
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));
            } else if (analytic.equals("louvain")) {
                DGAConfiguration partialConfiguration = DGAConfiguration.coalesce(fileConf, commandLineConf);
                partialConfiguration.setDGAGiraphProperty("-eip", inputPath);
                partialConfiguration.setDGAGiraphProperty("-op", outputPath);
                LouvainRunner louvainRunner = new LouvainRunner();
                System.exit(louvainRunner.runUntilComplete(inputPath, outputPath, partialConfiguration));
            } else if (analytic.equals("lc")) {
                DGAConfiguration requiredConf = new DGAConfiguration();
                requiredConf.setDGAGiraphProperty("-eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eof", SimpleEdgeOutputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eip", inputPath);
                requiredConf.setDGAGiraphProperty("-op", outputPath);
                requiredConf.setDGAGiraphProperty("-esd", outputPath);
                DGAConfiguration finalConfiguration = DGAConfiguration.coalesce(fileConf, commandLineConf, requiredConf);
                String[] giraphArgs = finalConfiguration.convertToCommandLineArguments(LeafCompressionComputation.class.getCanonicalName());
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));
            } else if (analytic.equals("pr")) {
                DGAConfiguration requiredConf = new DGAConfiguration();
                requiredConf.setDGAGiraphProperty("-eif", DGATextEdgeValueInputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eof", SimpleEdgeOutputFormat.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-eip", inputPath);
                requiredConf.setDGAGiraphProperty("-mc", PageRankMasterCompute.class.getCanonicalName());
                requiredConf.setDGAGiraphProperty("-op", outputPath);
                requiredConf.setDGAGiraphProperty(SimpleEdgeOutputFormat.SIMPLE_WRITE_SOURCE_VALUE, "true");
                DGAConfiguration finalConf = DGAConfiguration.coalesce(fileConf, commandLineConf, requiredConf);
                String[] giraphArgs = finalConf.convertToCommandLineArguments(PageRankComputation.class.getCanonicalName());
                System.exit(ToolRunner.run(new GiraphRunner(), giraphArgs));
            }

        } catch (Exception e) {
            logger.error("Unable to run analytic; ", e);
            System.exit(1);
        }
    }

}
