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
