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

import com.soteradefense.dga.hbse.VertexData;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class HBSEOutputFormatTest extends HBSEOutputFormat {
    private ImmutableClassesGiraphConfiguration<
            Text,VertexData,Text> conf;
    @Before
    public void setUp() throws Exception {
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
        giraphConfiguration.setComputationClass(BasicComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<Text,VertexData,Text>(giraphConfiguration);
    }
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        super.createVertexWriter(context);
        final TextVertexWriter vw = mock(TextVertexWriter.class);
        return vw;
    }

    @Test
    public void testNewDelimiter() throws IOException, InterruptedException {
        Text expected = new Text("1");
        Text approx = new Text("0.0");
        testWriter(expected, approx);
    }

    private void testWriter(Text expected, Text approx) throws IOException, InterruptedException {
        TaskAttemptContext tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);

        Vertex<Text,VertexData,Text> vertex = mock(Vertex.class);
        when(vertex.getId()).thenReturn(new Text("1"));
        when(vertex.getValue()).thenReturn(new VertexData());
        // Create empty iterator == no edges
        final RecordWriter<Text,Text> tw = mock(RecordWriter.class);
        SBVertexWriter writer = new SBVertexWriter() {
            @Override
            protected RecordWriter<Text, Text> createLineRecordWriter(
                    TaskAttemptContext context) throws IOException, InterruptedException {
                return tw;
            }
        };
        writer.setConf(conf);
        writer.initialize(tac);
        writer.writeVertex(vertex);
        verify(tw).write(expected, approx);
    }
}
