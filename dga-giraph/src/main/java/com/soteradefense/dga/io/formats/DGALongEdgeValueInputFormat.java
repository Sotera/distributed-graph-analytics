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
package com.soteradefense.dga.io.formats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Implementation of the DGAAbstractEdgeInputFormat for LongWritables as the edgeValue
 * Will throw an IOException if our edgeValue can't be parsed to Long, then returns a LongWritable in the getValue(RawEdge) method.
 */
public class DGALongEdgeValueInputFormat extends DGAAbstractEdgeInputFormat<LongWritable> {

    private static final String DEFAULT_EDGE_VALUE = "1";

    public DGAAbstractEdgeReader<LongWritable> getEdgeReader() {
        return new DGALongEdgeValueReader();
    }

    public class DGALongEdgeValueReader extends DGAAbstractEdgeReader<LongWritable> {

        protected String getDefaultEdgeValue() {
            return DEFAULT_EDGE_VALUE;
        }

        protected void validateEdgeValue(RawEdge edge) throws IOException {
            try {
                Long.parseLong(edge.getEdgeValue());
            } catch (Exception e) {
                throw new IOException("Unable to convert the edge value into a Long: " + edge.getEdgeValue(), e);
            }
        }

        @Override
        protected LongWritable getValue(RawEdge edge) {
            return new LongWritable(Long.parseLong(edge.getEdgeValue()));
        }
    }

}
