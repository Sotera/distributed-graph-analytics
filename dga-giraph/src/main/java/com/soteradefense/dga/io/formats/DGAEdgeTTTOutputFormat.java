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
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DGAEdgeTTTOutputFormat extends DGAAbstractEdgeOutputFormat<Text, Text, Text> {

    @Override
    public TextEdgeWriter<Text, Text, Text> createEdgeWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        DGALoggingUtil.setDGALogLevel(getConf());
        return new TTTEdgeWriter();
    }

    /**
     * A Simple Edge Writer that writes each edge into a file on HDFS.
     */
    protected class TTTEdgeWriter extends DGAAbstractEdgeWriter<Text, Text, Text> {

        @Override
        public String getVertexValueAsString(Text vertexValue) {
            return vertexValue.toString();
        }

        @Override
        public String getEdgeValueAsString(Text edgeValue) {
            return edgeValue.toString();
        }
    }
}
