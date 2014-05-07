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
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * SimpleTsvEdgeInputFormat allows us to specify an edge format of up to 3 columns; source, destination[, weight].
 * <p/>
 * Class does not provide any capacity for input data that does not conform to a 2 or 3 columns of character separated value data.
 * <p/>
 * If weight is not provided by the data, this class will allow you to override the default weight.  If no default weight
 * is specified, then the default weight defaults to a VIntWritable of 1.
 * <p/>
 * Override the field separator in the GiraphConfiguration provided to this class by Giraph.
 * <p/>
 * Configurable values and their default value setting:
 * * simple.tsv.edge.delimiter = "\t"
 * * simple.tsv.edge.weight.default = 1
 * <p/>
 * Set acceptable values for either of these in the configuration to change behavior.  simple.tsv.edge.weight.default must be
 * parseable by java.lang.Integer.parseInt().
 */
public class SimpleTsvEdgeInputFormat extends TextEdgeInputFormat<Text, VIntWritable> {

    /**
     * Key we use in the GiraphConfiguration to denote our field delimiter
     */
    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    /**
     * Default value used if no field delimiter is specified via the GiraphConfiguration
     */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    /**
     * Key we use in the GiraphConfiguration to denote our default edge weight
     */
    public static final String EDGE_WEIGHT_VALUE = "simple.tsv.edge.weight.default";

    /**
     * Edge weight used by default if not provided by data or overridden in GiraphConfiguration
     */
    public static final String EDGE_WEIGHT_VALUE_DEFAULT = "1";

    @Override
    public EdgeReader<Text, VIntWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new SimpleTsvEdgeReader();
    }

    protected class SimpleTsvEdgeReader extends TextEdgeInputFormat<Text, VIntWritable>.TextEdgeReaderFromEachLineProcessed<Text> {
        private String delimiter;
        private VIntWritable defaultEdgeWeight;

        /**
         * Upon intialization, determines the field separator and default weight to use from the GiraphConfiguration
         *
         * @param inputSplit
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(inputSplit, context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            String edgeWeight = getConf().get(EDGE_WEIGHT_VALUE, EDGE_WEIGHT_VALUE_DEFAULT);

            try {
                defaultEdgeWeight = new VIntWritable(Integer.parseInt(edgeWeight));
            } catch (NumberFormatException e) {
                throw new IOException("The default edge weight provided (configuration parameter: " + EDGE_WEIGHT_VALUE + ", value: " + edgeWeight + ", could not be cast to integer.");
            }

        }

        @Override
        protected Text preprocessLine(Text line) throws IOException {
            return new Text(line.toString().trim());
        }

        @Override
        protected Text getTargetVertexId(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length != 2 && splitValues.length != 3)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires 2 or 3 values.  Data row was [" + value + "]");
            return new Text(splitValues[1].trim());
        }

        @Override
        protected Text getSourceVertexId(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length != 2 && splitValues.length != 3)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires 2 or 3 values.  Data row was [" + value + "]");
            return new Text(splitValues[0].trim());
        }

        @Override
        protected VIntWritable getValue(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length != 2 && splitValues.length != 3)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires 2 or 3 values.  Data row was [" + value + "]");
            if (splitValues.length == 2) {
                return defaultEdgeWeight;
            }
            try {
                return new VIntWritable(Integer.parseInt(splitValues[2].trim()));
            } catch (NumberFormatException e) {
                throw new IOException("Row of data contained an invalid edge weight, [" + splitValues[2].trim() + "]");
            }
        }
    }
}
