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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextEdgeOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * The SimpleTsvEdgeOutputFormat outputs the edges that make up our graph.
 *
 * TODO: I really dislike the edge.getValue() that we output here -- we should discuss appropriate behavior at a later date.
 */
public class SimpleTsvEdgeOutputFormat extends TextEdgeOutputFormat<Text, Text, Text> {

    /**
     * Configuration Identifier for the file delimiter.
     */
    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    /**
     * The default value for the file delimiter.
     */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    /**
     * Configuration Identifier to use the source value when outputting.
     */
    public static final String SIMPLE_TSV_USE_SOURCE_VALUE = "simple.tsv.use.source.value";

    /**
     * The default value for the Use Source Value Configuration.
     */
    public static final String SIMPLE_TSV_USE_SOURCE_VALUE_DEFAULT = "false";

    @Override
    public TextEdgeWriter<Text, Text, Text> createEdgeWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new SimpleTsvEdgeWriter();
    }

    /**
     * A Simple Edge Writer that writes each edge into a file on HDFS.
     */
    protected class SimpleTsvEdgeWriter extends TextEdgeWriterToEachLine<Text, Text, Text> {

        /**
         * Delimiter to use when separating values.
         */
        private String delimiter;

        /**
         * Flag that says whether or not to use the source value when outputting.
         */
        private boolean useSourceValue;

        /**
         * Upon intialization, determines the field separator and default weight to use from the GiraphConfiguration
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            useSourceValue = Boolean.parseBoolean(getConf().get(SIMPLE_TSV_USE_SOURCE_VALUE, SIMPLE_TSV_USE_SOURCE_VALUE_DEFAULT));
        }

        /**
         * Converts an edge to a writable line.
         * @param sourceId Vertex Id
         * @param sourceValue Vertex Value
         * @param edge Edge that it is writing.
         * @return A Text value formatted to be a line.
         * @throws IOException
         */
        @Override
        protected Text convertEdgeToLine(Text sourceId, Text sourceValue, Edge<Text, Text> edge) throws IOException {
            if(!useSourceValue) {
                return new Text(sourceId + delimiter + edge.getTargetVertexId() + delimiter + edge.getValue().toString());
            }
            else{
                return new Text(sourceId.toString().trim() + delimiter + edge.getTargetVertexId().toString().trim() + delimiter + sourceValue.toString().trim());
            }
        }

    }
}
