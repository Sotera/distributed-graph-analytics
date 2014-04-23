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
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SimpleTsvEdgeInputFormatTest extends SimpleTsvEdgeInputFormat {

    private RecordReader<LongWritable, Text> rr;
    private ImmutableClassesGiraphConfiguration conf;
    private TaskAttemptContext tac;

    public EdgeReader<Text, VIntWritable> createEdgeReader(final RecordReader<LongWritable, Text> rr) throws IOException {
        return new SimpleTsvEdgeReader() {
            @Override
            protected RecordReader<LongWritable, Text> createLineRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
                return rr;
            }
        };
    }

    @Before
    public void setUp() throws IOException, InterruptedException {
        rr = mock(RecordReader.class);
        when(rr.nextKeyValue()).thenReturn(true);
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        conf = new ImmutableClassesGiraphConfiguration(giraphConf);
        tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);
    }

    @Test
    public void testEdgesWithoutWeight() throws Exception {
        String line1 = "1\t2";
        String line2 = "2\t4";
        when(rr.getCurrentValue()).thenReturn(new Text(line1)).thenReturn(new Text(line2));
        EdgeReader ter = createEdgeReader(rr);

        ter.setConf(conf);
        ter.initialize(null, tac);

        ter.nextEdge();
        assertEquals(new Text("1"), ter.getCurrentSourceId());
        assertEquals(new Text("2"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(1), ter.getCurrentEdge().getValue());

        ter.nextEdge();
        assertEquals(new Text("2"), ter.getCurrentSourceId());
        assertEquals(new Text("4"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(1), ter.getCurrentEdge().getValue());
    }

    @Test
    public void testEdgesWithWeight() throws Exception {
        String line1 = "1\t2\t2";
        String line2 = "2\t4\t1";
        when(rr.getCurrentValue()).thenReturn(new Text(line1)).thenReturn(new Text(line2));
        EdgeReader ter = createEdgeReader(rr);

        ter.setConf(conf);
        ter.initialize(null, tac);

        ter.nextEdge();
        assertEquals(new Text("1"), ter.getCurrentSourceId());
        assertEquals(new Text("2"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(2), ter.getCurrentEdge().getValue());

        ter.nextEdge();
        assertEquals(new Text("2"), ter.getCurrentSourceId());
        assertEquals(new Text("4"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(1), ter.getCurrentEdge().getValue());
    }

    @Test(expected = IOException.class)
    public void testEdgesWithInvalidWeight() throws Exception {
        String line1 = "1\t2\talphabet";
        String line2 = "2\t4\t1";
        when(rr.getCurrentValue()).thenReturn(new Text(line1)).thenReturn(new Text(line2));
        EdgeReader ter = createEdgeReader(rr);

        ter.setConf(conf);
        ter.initialize(null, tac);

        ter.nextEdge();
        ter.getCurrentEdge(); // throws IOException because "alphabet" isn't a valid int
        fail("Should throw IOException before this point");
    }

    @Test
    public void testEdgesWithWeightInSomeRowsAndNoWeightInOthers() throws Exception {
        String line1 = "1\t2\t2";
        String line2 = "2\t4";
        when(rr.getCurrentValue()).thenReturn(new Text(line1)).thenReturn(new Text(line2));
        EdgeReader ter = createEdgeReader(rr);

        ter.setConf(conf);
        ter.initialize(null, tac);

        ter.nextEdge();
        assertEquals(new Text("1"), ter.getCurrentSourceId());
        assertEquals(new Text("2"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(2), ter.getCurrentEdge().getValue());

        ter.nextEdge();
        assertEquals(new Text("2"), ter.getCurrentSourceId());
        assertEquals(new Text("4"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(1), ter.getCurrentEdge().getValue());
    }

    @Test(expected = IOException.class)
    public void testInvalidEdges() throws Exception {
        String line1 = "1\t";
        when(rr.getCurrentValue()).thenReturn(new Text(line1));
        EdgeReader ter = createEdgeReader(rr);

        ter.setConf(conf);
        ter.initialize(null, tac);

        ter.nextEdge();
        ter.getCurrentEdge(); // should throw IOException
        fail("Should throw IOException before this point");
    }

    @Test
    public void testEdgesWithConfiguredDelimiter() throws Exception {
        String line1 = "1,2,2";
        String line2 = "2,4,3";
        when(rr.getCurrentValue()).thenReturn(new Text(line1)).thenReturn(new Text(line2));
        EdgeReader ter = createEdgeReader(rr);

        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.set(SimpleTsvEdgeInputFormat.LINE_TOKENIZE_VALUE, ",");

        ter.setConf(new ImmutableClassesGiraphConfiguration(giraphConf));
        ter.initialize(null, tac);

        ter.nextEdge();
        assertEquals(new Text("1"), ter.getCurrentSourceId());
        assertEquals(new Text("2"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(2), ter.getCurrentEdge().getValue());

        ter.nextEdge();
        assertEquals(new Text("2"), ter.getCurrentSourceId());
        assertEquals(new Text("4"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(3), ter.getCurrentEdge().getValue());
    }

    @Test
    public void testEdgesWithConfiguredDefaultWeight() throws Exception {
        String line1 = "1\t2";
        String line2 = "2\t4";
        when(rr.getCurrentValue()).thenReturn(new Text(line1)).thenReturn(new Text(line2));
        EdgeReader ter = createEdgeReader(rr);

        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.set(SimpleTsvEdgeInputFormat.EDGE_WEIGHT_VALUE, "100");

        ter.setConf(new ImmutableClassesGiraphConfiguration(giraphConf));
        ter.initialize(null, tac);

        ter.nextEdge();
        assertEquals(new Text("1"), ter.getCurrentSourceId());
        assertEquals(new Text("2"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(100), ter.getCurrentEdge().getValue());

        ter.nextEdge();
        assertEquals(new Text("2"), ter.getCurrentSourceId());
        assertEquals(new Text("4"), ter.getCurrentEdge().getTargetVertexId());
        assertEquals(new VIntWritable(100), ter.getCurrentEdge().getValue());
    }

    @Test(expected = IOException.class)
    public void testEdgesWithInvalidConfiguredDefaultWeight() throws Exception {

        String line1 = "1,2";
        String line2 = "2,4";
        when(rr.getCurrentValue()).thenReturn(new Text(line1)).thenReturn(new Text(line2));
        EdgeReader ter = createEdgeReader(rr);

        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.set(SimpleTsvEdgeInputFormat.EDGE_WEIGHT_VALUE, "HAM");

        ter.setConf(new ImmutableClassesGiraphConfiguration(giraphConf));
        ter.initialize(null, tac);

        ter.nextEdge();
        ter.getCurrentEdge();
        fail("Should have thrown an IOException by this point, as HAM is not a valid int, even if it is delicious");
    }

}
