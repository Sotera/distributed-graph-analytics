package com.soteradefense.dga.pr;

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankMasterCompute extends DefaultMasterCompute {

    public static final double EPSILON = 0.001;
    private static final Logger logger = LoggerFactory.getLogger(PageRankMasterCompute.class);

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        DGALoggingUtil.setDGALogLevel(this.getConf());
        registerAggregator(PageRankComputation.MAX_EPSILON, DoubleMaxAggregator.class);
    }


    @Override
    public void compute() {
        long step = this.getSuperstep();
        if (step > 1) {
            double maxDelta = ((DoubleWritable) this.getAggregatedValue(PageRankComputation.MAX_EPSILON)).get();
            logger.info("step: {}  max delta: {}", step, maxDelta);
            if (maxDelta < EPSILON) {
                this.haltComputation();
                logger.info("{} < {} halting computation", maxDelta, EPSILON);
            }
        }


    }

}
