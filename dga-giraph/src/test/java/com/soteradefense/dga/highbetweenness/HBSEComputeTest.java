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
package com.soteradefense.dga.highbetweenness;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;


public class HBSEComputeTest {
    public GiraphConfiguration getConf() {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(HBSEMasterCompute.class);
        conf.setComputationClass(HBSEComputation.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
        conf.set(HBSEMasterCompute.BETWEENNESS_OUTPUT_DIR, "tmp/output");
        conf.set(HBSEMasterCompute.BETWEENNESS_SET_STABILITY, "1");
        conf.set(HBSEMasterCompute.BETWEENNESS_SET_MAX_SIZE, "10");
        conf.set(HBSEMasterCompute.BETWEENNESS_SET_STABILITY_COUNTER, "3");
        conf.set(HBSEMasterCompute.PIVOT_BATCH_STRING, "1,2,3,4,5");
        conf.set(HBSEMasterCompute.PIVOT_BATCH_SIZE, "5");
        conf.set(HBSEMasterCompute.VERTEX_COUNT, "8");
        return conf;
    }

    @Test
    public void testComputeOutput() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<IntWritable, VertexData, IntWritable> input = getFirstTestGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<IntWritable, VertexData, IntWritable> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertEquals(output.getVertex(new IntWritable(2)).getValue().getApproxBetweenness() > 0.0, true);
        assertEquals(output.getVertex(new IntWritable(1)).getValue().getApproxBetweenness() > 0.0, true);
        assertEquals(output.getVertex(new IntWritable(3)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(4)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(5)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(6)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(7)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(8)).getValue().getApproxBetweenness() == 0.0, true);
    }

    @Test
    public void testGraphWithShortestPathOne() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<IntWritable, VertexData, IntWritable> input = getShortestPathOneTestGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<IntWritable, VertexData, IntWritable> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertEquals(output.getVertex(new IntWritable(1)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(2)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(3)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(4)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(5)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(6)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(7)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(8)).getValue().getApproxBetweenness() == 0.0, true);

    }

    @Test
    public void testTwoCriticalPointGraph() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<IntWritable, VertexData, IntWritable> input = getTwoCriticalPointGraph(conf);
        conf.set(HBSEMasterCompute.VERTEX_COUNT, "16");
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<IntWritable, VertexData, IntWritable> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(16, output.getVertices().size());
        assertEquals(output.getVertex(new IntWritable(1)).getValue().getApproxBetweenness() > 0.0, true);
        assertEquals(output.getVertex(new IntWritable(9)).getValue().getApproxBetweenness() > 0.0, true);
        assertEquals(output.getVertex(new IntWritable(2)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(3)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(4)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(5)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(6)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(7)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(8)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(10)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(11)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(12)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(13)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(14)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(15)).getValue().getApproxBetweenness() == 0.0, true);
        assertEquals(output.getVertex(new IntWritable(16)).getValue().getApproxBetweenness() == 0.0, true);

    }

    private TestGraph<IntWritable, VertexData, IntWritable> getTwoCriticalPointGraph(GiraphConfiguration conf) {
        TestGraph<IntWritable, VertexData, IntWritable> testGraph = new TestGraph<IntWritable, VertexData, IntWritable>(conf);
        testGraph.addEdge(new IntWritable(1), new IntWritable(2), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(3), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(4), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(5), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(6), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(7), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(8), new IntWritable(1));
        testGraph.addEdge(new IntWritable(9), new IntWritable(10), new IntWritable(1));
        testGraph.addEdge(new IntWritable(9), new IntWritable(11), new IntWritable(1));
        testGraph.addEdge(new IntWritable(9), new IntWritable(12), new IntWritable(1));
        testGraph.addEdge(new IntWritable(9), new IntWritable(13), new IntWritable(1));
        testGraph.addEdge(new IntWritable(9), new IntWritable(14), new IntWritable(1));
        testGraph.addEdge(new IntWritable(9), new IntWritable(15), new IntWritable(1));
        testGraph.addEdge(new IntWritable(9), new IntWritable(16), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(9), new IntWritable(1));
        testGraph.addEdge(new IntWritable(9), new IntWritable(1), new IntWritable(1));
        return testGraph;
    }

    private TestGraph<IntWritable, VertexData, IntWritable> getShortestPathOneTestGraph(GiraphConfiguration conf) {
        TestGraph<IntWritable, VertexData, IntWritable> testGraph = new TestGraph<IntWritable, VertexData, IntWritable>(conf);
        testGraph.addEdge(new IntWritable(1), new IntWritable(2), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(3), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(4), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(5), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(6), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(7), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(8), new IntWritable(1));
        return testGraph;
    }

    private TestGraph<IntWritable, VertexData, IntWritable> getFirstTestGraph(GiraphConfiguration conf) {
        TestGraph<IntWritable, VertexData, IntWritable> testGraph = new TestGraph<IntWritable, VertexData, IntWritable>(conf);
        testGraph.addEdge(new IntWritable(1), new IntWritable(2), new IntWritable(1));
        testGraph.addEdge(new IntWritable(2), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(3), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(4), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(5), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(6), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(7), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(2), new IntWritable(8), new IntWritable(1));
        return testGraph;
    }
}
