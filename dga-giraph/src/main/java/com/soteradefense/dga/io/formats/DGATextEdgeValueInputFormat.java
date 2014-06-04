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

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Implementation of the DGAAbstractEdgeInputFormat for Text as the edgeValue
 * Does no verification of the edgeValue and simply returns the third value as a Text
 */
public class DGATextEdgeValueInputFormat extends DGAAbstractEdgeInputFormat<Text> {

    private static final String DEFAULT_EDGE_VALUE = "";

    /**
     * The create edge reader first determines if we should reverse each edge; some data sets are undirected graphs
     * and need the input format to reverse their connected nature.
     *
     * Calls the abstract method getEdgeReader() which will give us the appropriate EdgeReader for the subclass.
     * @param split
     * @param context
     * @return
     * @throws IOException
     */
    public EdgeReader<Text, Text> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        String duplicator = getConf().get(IO_EDGE_REVERSE_DUPLICATOR, IO_EDGE_REVERSE_DUPLICATOR_DEFAULT);
        boolean useDuplicator = Boolean.parseBoolean(duplicator);
        EdgeReader<Text, Text> reader = useDuplicator ? new ReverseEdgeDuplicator<Text, Text>(new DGATextEdgeValueReader()) : new DGATextEdgeValueReader();
        return reader;
    }

    public class DGATextEdgeValueReader extends DGAAbstractEdgeReader<Text> {

        @Override
        protected String getDefaultEdgeValue() {
            return DEFAULT_EDGE_VALUE;
        }

        protected void validateEdgeValue(RawEdge edge) throws IOException {
            // do nothing method, has no utility for Strings/Texts
        }

        @Override
        protected Text getValue(RawEdge edge) throws IOException {
            return new Text(edge.getEdgeValue());
        }
    }

}
