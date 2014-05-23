package com.soteradefense.dga.pagerank;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankMasterCompute extends DefaultMasterCompute {

    public static final double EPSILON = 0.001;

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerAggregator(PageRankCompute.MAX_EPSILON, DoubleMaxAggregator.class);
    }


    @Override
    public void compute() {
        long step = this.getSuperstep();
        if (step > 1) {
            Logger logger = LoggerFactory.getLogger(PageRankMasterCompute.class);
            double maxDelta = ((DoubleWritable) this.getAggregatedValue(PageRankCompute.MAX_EPSILON)).get();
            logger.info("step: {}  max delta: {}", step, maxDelta);
            if (maxDelta < EPSILON) {
                this.haltComputation();
                logger.info("{} < {} halting computation", maxDelta, EPSILON);
            }
        }


    }

}
