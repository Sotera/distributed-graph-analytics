/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * WeaklyConnectedComponentEdgeOutputFormat is a class that extends {@link org.apache.giraph.io.formats.TextEdgeOutputFormat}
 * to output an edge and a connected component value.
 *
 * Configurable values and their default settings:
 *      * simple.tsv.edge.delimiter = "\t"
 */
public class WeaklyConnectedComponentEdgeOutputFormat extends TextEdgeOutputFormat<Text, Text, NullWritable> {
    /** Key in {@link org.apache.giraph.conf.GiraphConfiguration} to specify the delimiter */
    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    /** The default delimiter value that is returned if the key is not set in {@link org.apache.giraph.conf.GiraphConfiguration} */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    @Override
    public TextEdgeWriter<Text, Text, NullWritable> createEdgeWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new EdgeAndVertexValueWriter();
    }
    /**
     * EdgeAndVertexValueWriter is a class that takes each edge that was read in and outputs the edge and a vertex value.
     */
    protected class EdgeAndVertexValueWriter extends TextEdgeWriterToEachLine<Text,Text,NullWritable>{
        private String delimiter;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
        }

        @Override
        protected Text convertEdgeToLine(Text sourceId, Text sourceValue, Edge<Text, NullWritable> edge) throws IOException {
            return new Text(sourceId.toString().trim() + delimiter + edge.getTargetVertexId().toString().trim() + delimiter + sourceValue.toString().trim());
        }
    }
}
