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

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Reads a comma separated, line delimited, edge file from HDFS.
 * <p/>
 * WARNINGS / NOTES
 * <p/>
 * id (src/target) values for all vertices must be in 0,1,2,...N where N=vertex.count-1 as set in the job configuration.
 * <p/>
 * The input file is read as a DIRECTED GRAPH. If you want to run
 * over an undirected graph you'll need to pre-process your input to add edges
 * in both directions where needed, or set the io.edge.reverse.duplicator custom argument to true.
 * <p/>
 * Format:
 * src,target,edgeValue
 * OR
 * src,target (if no edgeValue is specified a edgeValue of 1 is assumed.
 */
public class SimpleEdgeInputFormat extends TextEdgeInputFormat<Text, Text> {

    /**
     * Key we use in the GiraphConfiguration to denote our field delimiter
     */
    public static final String LINE_TOKENIZE_VALUE = "simple.edge.delimiter";

    /**
     * Default value used if no field delimiter is specified via the GiraphConfiguration
     */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = ",";

    /**
     * Key we use in the GiraphConfiguration to denote our default edge edgeValue
     */
    public static final String EDGE_VALUE = "simple.edge.value.default";

    /**
     * Edge edgeValue used by default if not provided by data or overridden in GiraphConfiguration
     */
    public static final String EDGE_VALUE_DEFAULT = "";

    /**
     * Configuration Identifier to use a reverse edge.
     */
    public static final String IO_EDGE_REVERSE_DUPLICATOR = "io.edge.reverse.duplicator";

    /**
     * Default Value for the reverse edge duplicator.
     */
    public static final String IO_EDGE_REVERSE_DUPLICATOR_DEFAULT = "false";

    @Override
    public EdgeReader<Text, Text> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        String duplicator = getConf().get(IO_EDGE_REVERSE_DUPLICATOR, IO_EDGE_REVERSE_DUPLICATOR_DEFAULT);
        boolean useDuplicator = Boolean.parseBoolean(duplicator);
        EdgeReader<Text, Text> reader = (useDuplicator) ? new ReverseEdgeDuplicator<Text, Text>(new SimpleEdgeReader()) : new SimpleEdgeReader();
        return reader;
    }

    /**
     * A SimpleEdge for encapsulating data.
     */
    private class SimpleEdge {

        private String src;

        private String target;

        private String edgeValue;

        public SimpleEdge(String[] tokens, String defaultEdgeValue) {
            src = tokens[0];
            target = tokens[1];
            edgeValue = (tokens.length > 2) ? tokens[2] : defaultEdgeValue;
        }
    }

    /**
     * An EdgeReader that uses SimpleEdge to process each line.
     */
    public class SimpleEdgeReader extends TextEdgeReaderFromEachLineProcessed<SimpleEdge> {

        private String delimiter;

        private String defaultEdgeValue;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(inputSplit, context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            defaultEdgeValue = getConf().get(EDGE_VALUE, EDGE_VALUE_DEFAULT);
        }

        @Override
        protected SimpleEdge preprocessLine(Text line) throws IOException {
            String[] tokens = line.toString().split(delimiter);
            return new SimpleEdge(tokens, defaultEdgeValue);
        }

        @Override
        protected Text getTargetVertexId(SimpleEdge line) throws IOException {
            return new Text(line.target);
        }

        @Override
        protected Text getSourceVertexId(SimpleEdge line) throws IOException {
            return new Text(line.src);
        }

        @Override
        protected Text getValue(SimpleEdge line) throws IOException {
            return new Text(line.edgeValue);
        }

    }


}
