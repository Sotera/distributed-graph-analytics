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
package com.soteradefense.dga.pr;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class PageRankTest {
    public GiraphConfiguration getConf() {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(PageRankMasterCompute.class);
        conf.setComputationClass(PageRankComputation.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
        //conf.setInt("mapreduce.job.counters.limit", 1000);
        return conf;
    }

    @Test
    public void testEqualPageRankForSevenNodes() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<Text, DoubleWritable, Text> input = getTestGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, DoubleWritable, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertTrue(output.getVertex(new Text("1")).getValue().get() < output.getVertex(new Text("8")).getValue().get());
        assertTrue(output.getVertex(new Text("1")).getValue().get() < output.getVertex(new Text("7")).getValue().get());
        assertTrue(output.getVertex(new Text("1")).getValue().get() < output.getVertex(new Text("6")).getValue().get());
        assertTrue(output.getVertex(new Text("1")).getValue().get() < output.getVertex(new Text("5")).getValue().get());
        assertTrue(output.getVertex(new Text("1")).getValue().get() < output.getVertex(new Text("4")).getValue().get());
        assertTrue(output.getVertex(new Text("1")).getValue().get() < output.getVertex(new Text("3")).getValue().get());
        assertTrue(output.getVertex(new Text("1")).getValue().get() < output.getVertex(new Text("2")).getValue().get());

    }

    @Test
    public void testHighPageRankForOneNode() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<Text, DoubleWritable, Text> input = getHighPageRankGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, DoubleWritable, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertTrue(output.getVertex(new Text("8")).getValue().get() < output.getVertex(new Text("1")).getValue().get());
        assertTrue(output.getVertex(new Text("2")).getValue().get() < output.getVertex(new Text("1")).getValue().get());
        assertTrue(output.getVertex(new Text("3")).getValue().get() < output.getVertex(new Text("1")).getValue().get());
        assertTrue(output.getVertex(new Text("4")).getValue().get() < output.getVertex(new Text("1")).getValue().get());
        assertTrue(output.getVertex(new Text("5")).getValue().get() < output.getVertex(new Text("1")).getValue().get());
        assertTrue(output.getVertex(new Text("6")).getValue().get() < output.getVertex(new Text("1")).getValue().get());
        assertTrue(output.getVertex(new Text("7")).getValue().get() < output.getVertex(new Text("1")).getValue().get());

    }


    private TestGraph<Text, DoubleWritable, Text> getHighPageRankGraph(GiraphConfiguration conf) {
        TestGraph<Text, DoubleWritable, Text> testGraph = new TestGraph<Text, DoubleWritable, Text>(conf);
        testGraph.addEdge(new Text("2"), new Text("1"), new Text());
        testGraph.addEdge(new Text("3"), new Text("1"), new Text());
        testGraph.addEdge(new Text("4"), new Text("1"), new Text());
        testGraph.addEdge(new Text("5"), new Text("1"), new Text());
        testGraph.addEdge(new Text("6"), new Text("1"), new Text());
        testGraph.addEdge(new Text("7"), new Text("1"), new Text());
        testGraph.addEdge(new Text("8"), new Text("1"), new Text());
        return testGraph;
    }

    private TestGraph<Text, DoubleWritable, Text> getTestGraph(GiraphConfiguration conf) {
        TestGraph<Text, DoubleWritable, Text> testGraph = new TestGraph<Text, DoubleWritable, Text>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), new Text());
        testGraph.addEdge(new Text("1"), new Text("3"), new Text());
        testGraph.addEdge(new Text("1"), new Text("4"), new Text());
        testGraph.addEdge(new Text("1"), new Text("5"), new Text());
        testGraph.addEdge(new Text("1"), new Text("6"), new Text());
        testGraph.addEdge(new Text("1"), new Text("7"), new Text());
        testGraph.addEdge(new Text("1"), new Text("8"), new Text());
        return testGraph;
    }

}
