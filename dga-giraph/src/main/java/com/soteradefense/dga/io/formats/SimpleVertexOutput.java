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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * SimpleVertexOutput is a simple vertex output class that outputs a vertex and it's value
 * separated by a delimiter specified in {@link org.apache.giraph.conf.GiraphConfiguration}.
 *
 * Configurable values and their default setting:
 *      * simple.tsv.vertex.delimiter = "\t"
 *
 * This writer simply writes each vertex and it's value.
 */
public class SimpleVertexOutput extends TextVertexOutputFormat<Text,Text,NullWritable> {
    /** Key in {@link org.apache.giraph.conf.GiraphConfiguration} to specify the delimiter */
    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.vertex.delimiter";

    /** The default delimiter value that is returned if the key is not set in {@link org.apache.giraph.conf.GiraphConfiguration} */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new VertexOutput();
    }
    /**
     * VertexOutput is a class that formats the specific output for the {@link com.soteradefense.dga.io.formats.SimpleVertexOutput}.
     */
    protected class VertexOutput extends TextVertexWriterToEachLine{

        private String delimiter;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
        }

        @Override
        protected Text convertVertexToLine(Vertex<Text, Text, NullWritable> vertex) throws IOException {
            return new Text(vertex.getId() + delimiter + vertex.getValue());
        }
    }
}
