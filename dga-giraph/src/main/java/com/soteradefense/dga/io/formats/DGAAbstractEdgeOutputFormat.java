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

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextEdgeOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * <p>The EdgeTDTOutputFormat outputs the edges that make up our graph.<p/>
 * <p>This output format will allow users to write out the source vertex ID, the destination vertex ID, and optionally, the vertex value and/or the edge value, using an optionally defined field delimiter (defaults to ,) <p/>
 */
@SuppressWarnings("unchecked")
public abstract class DGAAbstractEdgeOutputFormat<I extends WritableComparable, V extends Writable, E extends Writable> extends TextEdgeOutputFormat<I, V, E> {

    /**
     * Configuration Identifier for the file delimiter.
     */
    public static final String FIELD_DELIMITER = "edge.delimiter";

    /**
     * The default value for the file delimiter.
     */
    public static final String FIELD_DELIMITER_DEFAULT = ",";

    /**
     * Configuration Identifier to use the source value when outputting.
     */
    public static final String WRITE_VERTEX_VALUE = "write.vertex.value";

    /**
     * The default value for the Use Source Value Configuration.
     */
    public static final String WRITE_VERTEX_VALUE_DEFAULT = "false";

    /**
     * Configuration Identifier to use the source value when outputting.
     */
    public static final String WRITE_EDGE_VALUE = "write.edge.value";

    /**
     * The default value for the Use Source Value Configuration.
     */
    public static final String WRITE_EDGE_VALUE_DEFAULT = "false";

    /**
     * A Edge Writer that writes each edge into a file on HDFS.
     */
    protected abstract class DGAAbstractEdgeWriter<I extends WritableComparable, V extends Writable, E extends Writable> extends TextEdgeWriterToEachLine<I, V, E> {

        /**
         * Delimiter to use when separating values.
         */
        private String delimiter;

        /**
         * Flag that says whether or not to use the source value when outputting.
         */
        private boolean useVertexValue;

        /**
         * Flag that says whether or not to use the edge value when outputting.
         */
        private boolean useEdgeValue;

        /**
         * Upon intialization, determines the field separator and default weight to use from the GiraphConfiguration
         *
         * @param context
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = context.getConfiguration().get(FIELD_DELIMITER, FIELD_DELIMITER_DEFAULT);
            useVertexValue = Boolean.parseBoolean(context.getConfiguration().get(WRITE_VERTEX_VALUE, WRITE_VERTEX_VALUE_DEFAULT));
            useEdgeValue = Boolean.parseBoolean(context.getConfiguration().get(WRITE_EDGE_VALUE, WRITE_EDGE_VALUE_DEFAULT));
        }

        /**
         * The implementing classes must convert their Writable object into a String that can be appended to the String of text that will become our Text output
         * @param vertexValue
         * @return
         */
        public abstract String getVertexValueAsString(V vertexValue);

        /**
         * The implementing classes must convert their Writable object into a String that can be appended to the String of text that will become our Text output
         * @param edgeValue
         * @return
         */
        public abstract String getEdgeValueAsString(E edgeValue);

        @Override
        protected Text convertEdgeToLine(I sourceId, V vertexValue, Edge<I, E> edge) throws IOException {
            StringBuilder builder = new StringBuilder();
            builder.append(sourceId).append(delimiter).append(edge.getTargetVertexId());
            if (useVertexValue) {
                builder.append(delimiter).append(getVertexValueAsString(vertexValue));
            }
            if (useEdgeValue) {
                builder.append(delimiter).append(getEdgeValueAsString(edge.getValue()));
            }
            return new Text(builder.toString());
        }

    }
}
