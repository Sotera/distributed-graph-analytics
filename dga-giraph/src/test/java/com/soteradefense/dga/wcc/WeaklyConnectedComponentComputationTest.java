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
package com.soteradefense.dga.wcc;


import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WeaklyConnectedComponentComputationTest {

    @Test
    public void testComputeOutput() throws Exception {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(WeaklyConnectedComponentComputation.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
        TestGraph<Text,Text,Text> testGraph = getGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, testGraph);
        TestGraph<Text, Text, Text> graph = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(6, graph.getVertices().size());
        assertEquals("2", graph.getVertex(new Text("1")).getValue().toString());
        assertEquals("2", graph.getVertex(new Text("2")).getValue().toString());
        assertEquals("4", graph.getVertex(new Text("3")).getValue().toString());
        assertEquals("4", graph.getVertex(new Text("4")).getValue().toString());
        assertEquals("6", graph.getVertex(new Text("5")).getValue().toString());
        assertEquals("6", graph.getVertex(new Text("6")).getValue().toString());
    }
    private TestGraph<Text,Text,Text> getGraph(GiraphConfiguration conf){
        TestGraph<Text, Text, Text> testGraph = new TestGraph<Text, Text, Text>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), new Text(""));
        testGraph.addEdge(new Text("2"), new Text("1"), new Text(""));
        testGraph.addEdge(new Text("3"), new Text("4"), new Text(""));
        testGraph.addEdge(new Text("4"), new Text("3"), new Text(""));
        testGraph.addEdge(new Text("5"), new Text("6"), new Text(""));
        testGraph.addEdge(new Text("6"), new Text("5"), new Text(""));
        return testGraph;
    }
}
