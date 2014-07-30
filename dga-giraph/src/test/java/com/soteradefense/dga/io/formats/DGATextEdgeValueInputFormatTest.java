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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DGATextEdgeValueInputFormatTest extends DGATextEdgeValueInputFormat {
    private RecordReader<LongWritable,Text> rr;
    private ImmutableClassesGiraphConfiguration<Text, Text, Text> conf;
    private TaskAttemptContext tac;

    @Before
    public void setUp() throws IOException, InterruptedException {
        rr = mock(RecordReader.class);
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(BasicComputation.class);
        giraphConf.set(DGALongEdgeValueInputFormat.LINE_TOKENIZE_VALUE, ",");
        conf = new ImmutableClassesGiraphConfiguration<Text, Text, Text>(giraphConf);
        tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);
    }

    public EdgeReader<Text, Text> createEdgeReader(final RecordReader<LongWritable,Text> rr) throws IOException {
        return new DGATextEdgeValueReader(){
            @Override
            protected RecordReader<LongWritable, Text> createLineRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
                return rr;
            }
        };
    }

    @Test
    public void testInputParserWithDefaultWeight() throws IOException, InterruptedException {
        String input = "1,2";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text("1"));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text("2"));
        assertEquals(ter.getCurrentEdge().getValue(), new Text(""));

    }

    @Test
    public void testInputParserWithDefaultWeightAndOverriddenSeparator() throws IOException, InterruptedException {
        String input = "1\t2";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        conf.set(LINE_TOKENIZE_VALUE, "\t");
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text("1"));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text("2"));
        assertEquals(ter.getCurrentEdge().getValue(), new Text());

    }

    @Test
    public void testInputParserWithCustomWeight() throws IOException, InterruptedException {
        String input = "1,2,10";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text("1"));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text("2"));
        assertEquals(ter.getCurrentEdge().getValue(), new Text("10"));

    }

    @Test
    public void testInputParserIgnoreWeight() throws IOException, InterruptedException {
        String input = "1,2,10";
        conf.set(IO_IGNORE_THIRD, "true");
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text("1"));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text("2"));
        assertEquals(ter.getCurrentEdge().getValue(), new Text(""));

    }

    @Test
    public void testInputParserWithCustomWeightAndOverriddenSeparator() throws IOException, InterruptedException {
        String input = "1\t2\t10";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        conf.set(LINE_TOKENIZE_VALUE, "\t");
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text("1"));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text("2"));
        assertEquals(ter.getCurrentEdge().getValue(), new Text("10"));

    }

    @Test
    public void testInputParserWithDelimiterInData() throws IOException, InterruptedException {
        String input = "te\\tst@test.com\tanother@test.com\t10";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        conf.set(LINE_TOKENIZE_VALUE, "\t");
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text("te\\tst@test.com"));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text("another@test.com"));
        assertEquals(ter.getCurrentEdge().getValue(), new Text("10"));

    }
    @Test
    public void testInputParserWithDelimiterInDataNoEscape() throws IOException, InterruptedException {
        String input = "te\tst@test.com\tanother@test.com\t10";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        conf.set(LINE_TOKENIZE_VALUE, "\t");
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text("te"));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text("st@test.com"));
        assertEquals(ter.getCurrentEdge().getValue(), new Text("another@test.com"));

    }
    @Test(expected=IOException.class)
    public void testInputParserWithMalformedLine() throws IOException, InterruptedException {
        String input = "1";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text("1"));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text());
        assertEquals(ter.getCurrentEdge().getValue(), new Text());

    }
    @Test(expected=IOException.class)
    public void testInputParserWithMalformedLineAndDelimiter() throws IOException, InterruptedException {
        String input = "1,";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text("1"));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text());
        assertEquals(ter.getCurrentEdge().getValue(), new Text());

    }
    @Test(expected=IOException.class)
    public void testInputParserWithMalformedLineAndDelimiterNoSource() throws IOException, InterruptedException {
        String input = ",1";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new Text());
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new Text("1"));
        assertEquals(ter.getCurrentEdge().getValue(), new Text());

    }
}
