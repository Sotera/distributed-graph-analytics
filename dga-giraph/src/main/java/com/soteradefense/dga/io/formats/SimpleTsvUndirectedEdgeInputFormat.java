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


import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * SimpleTsvUndirectedEdgeInputFormat is a class that reads in an edge list specified by source, target, and other values.
 * The delimiter and expected number of columns are specified in the {@link org.apache.giraph.conf.GiraphConfiguration}.
 *
 * Configurable values and their default value setting:
 *      * simple.tsv.edge.delimiter = "\t"
 *      * simple.tsv.edge.column.count = 4
 *
 * If the reader detects that the line doesn't have the expected number of columns, it will throw an IOException.
 *
 * This input writer ignores the weight of an edge and makes the graph undirected by duplicating each edge read in.
 */
public class SimpleTsvUndirectedEdgeInputFormat extends TextEdgeInputFormat<Text, NullWritable> {

    /** Key in {@link org.apache.giraph.conf.GiraphConfiguration} to specify the delimiter */
    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    /** The default delimiter value that is returned if the key is not set in {@link org.apache.giraph.conf.GiraphConfiguration} */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    /** Key in {@link org.apache.giraph.conf.GiraphConfiguration} to specify the expected column count */
    public static final String EXPECTED_NUMBER_OF_COLUMNS_KEY = "simple.tsv.edge.column.count";

    /** The default column count if no count is specified in {@link org.apache.giraph.conf.GiraphConfiguration} */
    public static final String EXPECTED_NUMBER_OF_COLUMNS = "4";

    @Override
    public EdgeReader<Text, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new ReverseEdgeDuplicator<Text, NullWritable>(new SimpleTsvEdgeReader());
    }

    protected class SimpleTsvEdgeReader extends TextEdgeInputFormat<Text, NullWritable>.TextEdgeReaderFromEachLineProcessed<Text> {
        private String delimiter;
        private NullWritable defaultEdgeWeight;
        private int numberOfExpectedColumns;

        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(inputSplit, context);
            delimiter = context.getConfiguration().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            numberOfExpectedColumns = Integer.parseInt(context.getConfiguration().get(EXPECTED_NUMBER_OF_COLUMNS_KEY, EXPECTED_NUMBER_OF_COLUMNS));
            defaultEdgeWeight = NullWritable.get();
        }

        @Override
        protected Text preprocessLine(Text line) throws IOException {
            return new Text(line.toString().trim());
        }

        @Override
        protected Text getTargetVertexId(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length != numberOfExpectedColumns)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires " + String.valueOf(numberOfExpectedColumns) + " values.  Data row was [" + value + "]");
            if(splitValues[1].trim().length() == 0)
                throw new IOException("The target vertex is empty! The row is: " + value);
            return new Text(splitValues[1].trim());
        }

        @Override
        protected Text getSourceVertexId(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length != numberOfExpectedColumns)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires " + String.valueOf(numberOfExpectedColumns) + " values.  Data row was [" + value + "]");
            if(splitValues[0].trim().length() == 0)
                throw new IOException("The source vertex is empty! The row is: " + value);
            return new Text(splitValues[0].trim());
        }

        @Override
        protected NullWritable getValue(Text line) throws IOException {
            return defaultEdgeWeight;
        }
    }
}
