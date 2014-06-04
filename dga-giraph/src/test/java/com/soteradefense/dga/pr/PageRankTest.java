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

import static junit.framework.Assert.assertEquals;
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
        TestGraph<Text, Text, Text> input = getTestGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, Text, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertTrue(getDoubleWritable(output.getVertex(new Text("1")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("8")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("2")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("8")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("3")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("8")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("4")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("8")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("5")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("8")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("6")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("8")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("7")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("8")).getValue()).get());

    }

    @Test
    public void testHighPageRankForOneNode() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<Text, Text, Text> input = getHighPageRankGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, Text, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertTrue(getDoubleWritable(output.getVertex(new Text("8")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("1")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("2")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("1")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("3")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("1")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("4")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("1")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("5")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("1")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("6")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("1")).getValue()).get());
        assertTrue(getDoubleWritable(output.getVertex(new Text("7")).getValue()).get() < getDoubleWritable(output.getVertex(new Text("1")).getValue()).get());

    }

    private DoubleWritable getDoubleWritable(Text val) {
        return new DoubleWritable(Double.parseDouble(val.toString()));
    }

    private TestGraph<Text, Text, Text> getHighPageRankGraph(GiraphConfiguration conf) {
        TestGraph<Text, Text, Text> testGraph = new TestGraph<Text, Text, Text>(conf);
        testGraph.addEdge(new Text("2"), new Text("1"), new Text());
        testGraph.addEdge(new Text("3"), new Text("1"), new Text());
        testGraph.addEdge(new Text("4"), new Text("1"), new Text());
        testGraph.addEdge(new Text("5"), new Text("1"), new Text());
        testGraph.addEdge(new Text("6"), new Text("1"), new Text());
        testGraph.addEdge(new Text("7"), new Text("1"), new Text());
        testGraph.addEdge(new Text("8"), new Text("1"), new Text());
        return testGraph;
    }

    private TestGraph<Text, Text, Text> getTestGraph(GiraphConfiguration conf) {
        TestGraph<Text, Text, Text> testGraph = new TestGraph<Text, Text, Text>(conf);
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
