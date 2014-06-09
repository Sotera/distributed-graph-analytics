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
import java.util.Iterator;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class DGAEdgeTDTOutputFormatTest extends DGAEdgeTDTOutputFormat {

    private ImmutableClassesGiraphConfiguration conf;
    
    private TaskAttemptContext tac;

    private Vertex<Text, DoubleWritable, Text> vertex;

    Edge<Text, Text> edge1;

    Edge<Text, Text> edge2;

    private RecordWriter<Text, Text> rw;

    @Before
    public void setUp() throws Exception {
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
        conf = new ImmutableClassesGiraphConfiguration<Text, Text, Text>(giraphConfiguration);
        tac = mock(TaskAttemptContext.class);

        vertex = mock(Vertex.class);
        when(vertex.getId()).thenReturn(new Text("34"));
        when(vertex.getValue()).thenReturn(new DoubleWritable(10.43433333389));

        Iterable<Edge<Text, Text>> iterable = mock(Iterable.class);
        Iterator<Edge<Text, Text>> iterator = mock(Iterator.class);
        when(iterable.iterator()).thenReturn(iterator);

        edge1 = mock(Edge.class);
        when(edge1.getTargetVertexId()).thenReturn(new Text("12"));
        when(edge1.getValue()).thenReturn(new Text("1"));

        edge2 = mock(Edge.class);
        when(edge2.getTargetVertexId()).thenReturn(new Text("6"));
        when(edge2.getValue()).thenReturn(new Text("4"));

        rw = mock(RecordWriter.class);

        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(edge1, edge2);

    }

    public TextEdgeWriter<Text, DoubleWritable, Text> createEdgeWriter(final RecordWriter<Text, Text> rw) {
        return new TDTEdgeWriter() {
            @Override
            protected RecordWriter<Text, Text> createLineRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
                return rw;
            }
        };
    }

    @Test
    public void testWriteGraphAsEdges() throws Exception {
        TextEdgeWriter<Text, DoubleWritable, Text> writer = createEdgeWriter(rw);
        conf.set(WRITE_VERTEX_VALUE, "true");
        writer.setConf(conf);
        when(tac.getConfiguration()).thenReturn(conf);
        writer.initialize(tac);
        writer.writeEdge(vertex.getId(), vertex.getValue(), edge1);
        verify(rw).write(new Text("34\t12\t10.43433333389"), null);
        writer.writeEdge(vertex.getId(), vertex.getValue(), edge2);
        verify(rw).write(new Text("34\t6\t10.43433333389"), null);
    }

    @Test
    public void testWriteGraphWithOverriddenSeparator() throws Exception {
        TextEdgeWriter<Text, DoubleWritable, Text> writer = createEdgeWriter(rw);
        conf.set(DGAEdgeTTTOutputFormat.FIELD_DELIMITER, ":");
        conf.set(WRITE_EDGE_VALUE, "true");
        conf.set(WRITE_VERTEX_VALUE, "true");
        writer.setConf(conf);
        when(tac.getConfiguration()).thenReturn(conf);
        writer.initialize(tac);
        writer.writeEdge(vertex.getId(), vertex.getValue(), edge1);
        verify(rw).write(new Text("34:12:10.43433333389:1"), null);
        writer.writeEdge(vertex.getId(), vertex.getValue(), edge2);
        verify(rw).write(new Text("34:6:10.43433333389:4"), null);

    }

    @Test
    public void testGraphWriteWithEmptyEdgeWeight() throws Exception {
        TextEdgeWriter<Text, DoubleWritable, Text> writer = createEdgeWriter(rw);

        writer.setConf(conf);
        when(tac.getConfiguration()).thenReturn(conf);
        writer.initialize(tac);

        Edge<Text, Text> edge = mock(Edge.class);
        when(edge.getTargetVertexId()).thenReturn(new Text("12"));
        when(edge.getValue()).thenReturn(new Text());

        writer.writeEdge(vertex.getId(), vertex.getValue(), edge);
        verify(rw).write(new Text("34\t12"), null);

    }
}
