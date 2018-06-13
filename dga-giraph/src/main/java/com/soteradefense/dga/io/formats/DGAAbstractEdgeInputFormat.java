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
package com.soteradefense.dga.io.formats;

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Abstract class that simplifies the setup of our EdgeInputFormat subclasses.
 * <p/>
 * All DGA analytics require data to be specified in roughly the same way, while some algorithms may require a 3rd column
 * with a weight and others may not care.  The only duty of this class is to set up the edge reverse duplicator, and extract
 * defaults or overrides from the GiraphConfiguration
 *
 * @param <E>
 */
public abstract class DGAAbstractEdgeInputFormat<E extends Writable> extends TextEdgeInputFormat<Text, E> {

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
     * Configuration Identifier to use a reverse edge.
     */
    public static final String IO_EDGE_REVERSE_DUPLICATOR = "io.edge.reverse.duplicator";

    /**
     * Configuration Identifier for ignoring anything past the second column.
     */
    public static final String IO_IGNORE_THIRD = "simple.edge.column.ignore";

    /**
     * Default Value for the reverse edge duplicator.
     */
    public static final String IO_EDGE_REVERSE_DUPLICATOR_DEFAULT = "false";

    /**
     * Default Value for ignoring the third column.
     */
    public static final String IGNORE_THIRD_DEFAULT = "false";

    /**
     * Simple implementation that offloads work of parsing to the RawEdge class and the work of casting our edgeValue as
     * a Writable class of choice to the implementing subclasses.
     *
     * @param <E> Writable class to be stated in the implementing class.
     */
    public abstract class DGAAbstractEdgeReader<E extends Writable> extends TextEdgeReaderFromEachLineProcessed<RawEdge> {

        private String delimiter;

        private String defaultEdgeValue;

        private boolean ignoreThirdColumn;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(inputSplit, context);
            delimiter = getContext().getConfiguration().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            
            // Check if the delimiter is a control character that starts with 0x with a hex value
            if (delimiter.length() > 1 && delimiter.charAt(0) == 92 && delimiter.charAt(1) == 120)
                // Convert the hex value to decimal and then get the corresponding ASCII character
                delimiter = Character.toString((char) Integer.parseInt(delimiter.substring(2), 16));

            defaultEdgeValue = getContext().getConfiguration().get(EDGE_VALUE, getDefaultEdgeValue());
            ignoreThirdColumn = Boolean.parseBoolean(getContext().getConfiguration().get(IO_IGNORE_THIRD, IGNORE_THIRD_DEFAULT));
            DGALoggingUtil.setDGALogLevel(getContext().getConfiguration());
        }

        @Override
        protected RawEdge preprocessLine(Text line) throws IOException {
            RawEdge edge = new RawEdge(delimiter, getDefaultEdgeValue(), ignoreThirdColumn);
            edge.fromText(line);
            validateEdgeValue(edge);
            return edge;
        }

        protected abstract String getDefaultEdgeValue();

        protected abstract void validateEdgeValue(RawEdge edge) throws IOException;

        @Override
        protected Text getTargetVertexId(RawEdge edge) throws IOException {
            return new Text(edge.getTargetId());
        }

        @Override
        protected Text getSourceVertexId(RawEdge edge) throws IOException {
            return new Text(edge.getSourceId());
        }

    }

}
