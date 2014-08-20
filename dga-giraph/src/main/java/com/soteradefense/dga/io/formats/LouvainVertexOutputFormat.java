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
import com.soteradefense.dga.louvain.giraph.LouvainNodeState;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


/**
 * Outputs the graph as text in hdfs:
 * <p/>
 * Format is a tab seperated file with
 * id	community id	internal weight	community edge list
 * <p/>
 * the edge list is a comma seperated list of edges of the form    id:weight
 */
public class LouvainVertexOutputFormat extends TextVertexOutputFormat<Text, LouvainNodeState, LongWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
        DGALoggingUtil.setDGALogLevel(getConf());
        return new LouvainVertexWriter();
    }


    protected class LouvainVertexWriter extends TextVertexWriter {

        @Override
        public void writeVertex(Vertex<Text, LouvainNodeState, LongWritable> vertex) throws IOException, InterruptedException {
            StringBuilder b = new StringBuilder();
            b.append(vertex.getValue().getCommunity());
            b.append("\t");
            b.append(vertex.getValue().getInternalWeight());
            b.append("\t");

            for (Edge<Text, LongWritable> e : vertex.getEdges()) {
                b.append(e.getTargetVertexId());
                b.append(":");
                b.append(e.getValue());
                b.append(",");
            }
            b.deleteCharAt(b.length() - 1);

            getRecordWriter().write(vertex.getId(), new Text(b.toString()));

        }

    }


}
