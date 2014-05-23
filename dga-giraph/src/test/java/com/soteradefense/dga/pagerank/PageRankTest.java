package com.soteradefense.dga.pagerank;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class PageRankTest {
    public GiraphConfiguration getConf() {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(PageRankMasterCompute.class);
        conf.setComputationClass(PageRankCompute.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
        return conf;
    }

    @Test
    public void testEqualPageRankForSevenNodes() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<Text, DoubleWritable, NullWritable> input = getTestGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, DoubleWritable, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertTrue(output.getVertex(new Text("1")).getValue().get() < output.getVertex(new Text("8")).getValue().get());
        assertTrue(output.getVertex(new Text("2")).getValue().get() == output.getVertex(new Text("8")).getValue().get());
        assertTrue(output.getVertex(new Text("3")).getValue().get() == output.getVertex(new Text("8")).getValue().get());
        assertTrue(output.getVertex(new Text("4")).getValue().get() == output.getVertex(new Text("8")).getValue().get());
        assertTrue(output.getVertex(new Text("5")).getValue().get() == output.getVertex(new Text("8")).getValue().get());
        assertTrue(output.getVertex(new Text("6")).getValue().get() == output.getVertex(new Text("8")).getValue().get());
        assertTrue(output.getVertex(new Text("7")).getValue().get() == output.getVertex(new Text("8")).getValue().get());
    }

    @Test
    public void testHighPageRankForOneNode() throws Exception{
        GiraphConfiguration conf = getConf();
        TestGraph<Text, DoubleWritable, NullWritable> input = getHighPageRankGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf,input);
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
    private TestGraph<Text, DoubleWritable, NullWritable> getHighPageRankGraph(GiraphConfiguration conf) {
        TestGraph<Text, DoubleWritable, NullWritable> testGraph = new TestGraph<Text, DoubleWritable, NullWritable>(conf);
        testGraph.addEdge(new Text("2"), new Text("1"), NullWritable.get());
        testGraph.addEdge(new Text("3"), new Text("1"), NullWritable.get());
        testGraph.addEdge(new Text("4"), new Text("1"), NullWritable.get());
        testGraph.addEdge(new Text("5"), new Text("1"), NullWritable.get());
        testGraph.addEdge(new Text("6"), new Text("1"), NullWritable.get());
        testGraph.addEdge(new Text("7"), new Text("1"), NullWritable.get());
        testGraph.addEdge(new Text("8"), new Text("1"), NullWritable.get());
        return testGraph;
    }

    private TestGraph<Text, DoubleWritable, NullWritable> getTestGraph(GiraphConfiguration conf) {
        TestGraph<Text, DoubleWritable, NullWritable> testGraph = new TestGraph<Text, DoubleWritable, NullWritable>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), NullWritable.get());
        testGraph.addEdge(new Text("1"), new Text("3"), NullWritable.get());
        testGraph.addEdge(new Text("1"), new Text("4"), NullWritable.get());
        testGraph.addEdge(new Text("1"), new Text("5"), NullWritable.get());
        testGraph.addEdge(new Text("1"), new Text("6"), NullWritable.get());
        testGraph.addEdge(new Text("1"), new Text("7"), NullWritable.get());
        testGraph.addEdge(new Text("1"), new Text("8"), NullWritable.get());
        return testGraph;
    }
}
