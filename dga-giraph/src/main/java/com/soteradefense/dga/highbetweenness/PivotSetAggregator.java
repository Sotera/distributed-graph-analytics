/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.soteradefense.dga.highbetweenness;

import org.apache.giraph.aggregators.BasicAggregator;

/**
 * Stores a single TextArrayWritable for global broadcast / communication.
 */
public class PivotSetAggregator extends BasicAggregator<PivotSetWritable> {

    /**
     * Set the aggregated value of this aggregator to
     * the specified value.
     */
    public void aggregate(PivotSetWritable value) {
        if(value == null)
            return;
        PivotSetWritable old = getAggregatedValue();
        old.aggregate(value);
    }

    /**
     * Returns an empty TextArrayWritable
     */
    public PivotSetWritable createInitialValue() {
        return new PivotSetWritable();
    }

}
