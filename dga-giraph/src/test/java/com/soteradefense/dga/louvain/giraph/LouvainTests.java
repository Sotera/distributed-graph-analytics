package com.soteradefense.dga.louvain.giraph;

import com.soteradefense.dga.louvain.giraph.LouvainComputation;
import com.soteradefense.dga.louvain.giraph.LouvainMasterCompute;
import com.soteradefense.dga.louvain.giraph.LouvainNodeState;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class LouvainTests {

    public GiraphConfiguration getConf() {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(LouvainMasterCompute.class);
        conf.setComputationClass(LouvainComputation.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
        System.setProperty("giraph.useSuperstepCounters", "false");
        conf.set("actual.Q.aggregators", "1");
        conf.set("minimum.progress", "2000");
        conf.set("progress.tries", "1");
        conf.set("mapred.output.dir", "tmp/giraph_0");
        return conf;
    }

    @Test
    public void testUndirectedCase() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<Text, LouvainNodeState, LongWritable> input = getGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, LouvainNodeState, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertTrue(output.getVertices().size() == 2);
        assertTrue(output.getVertex(new Text("1")).getValue().getCommunity().equals("1"));
        assertTrue(output.getVertex(new Text("2")).getValue().getCommunity().equals("2"));
    }

    @Test
    public void testBiggerGraph() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<Text, LouvainNodeState, LongWritable> input = getBiggerGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, LouvainNodeState, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertTrue(output.getVertices().size() == 16);
        assertTrue(output.getVertex(new Text("1")).getValue().getCommunity().equals("1"));
        assertTrue(output.getVertex(new Text("2")).getValue().getCommunity().equals("2"));
        assertTrue(output.getVertex(new Text("3")).getValue().getCommunity().equals("3"));
        assertTrue(output.getVertex(new Text("4")).getValue().getCommunity().equals("4"));
        assertTrue(output.getVertex(new Text("5")).getValue().getCommunity().equals("5"));
        assertTrue(output.getVertex(new Text("6")).getValue().getCommunity().equals("6"));
        assertTrue(output.getVertex(new Text("7")).getValue().getCommunity().equals("7"));
        assertTrue(output.getVertex(new Text("8")).getValue().getCommunity().equals("8"));
        assertTrue(output.getVertex(new Text("9")).getValue().getCommunity().equals("9"));
        assertTrue(output.getVertex(new Text("10")).getValue().getCommunity().equals("10"));
        assertTrue(output.getVertex(new Text("11")).getValue().getCommunity().equals("11"));
        assertTrue(output.getVertex(new Text("12")).getValue().getCommunity().equals("12"));
        assertTrue(output.getVertex(new Text("13")).getValue().getCommunity().equals("13"));
        assertTrue(output.getVertex(new Text("14")).getValue().getCommunity().equals("14"));
        assertTrue(output.getVertex(new Text("15")).getValue().getCommunity().equals("15"));
        assertTrue(output.getVertex(new Text("16")).getValue().getCommunity().equals("16"));
    }

    @Test
    public void testCyclicGraph() throws Exception{
        GiraphConfiguration conf = getConf();
        TestGraph<Text, LouvainNodeState, LongWritable> input = getCyclicGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, LouvainNodeState, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertTrue(output.getVertices().size() == 3);
        assertTrue(output.getVertex(new Text("1")).getValue().getCommunity().equals("2"));
        assertTrue(output.getVertex(new Text("2")).getValue().getCommunity().equals("2"));
        assertTrue(output.getVertex(new Text("3")).getValue().getCommunity().equals("2"));
    }

    private TestGraph<Text, LouvainNodeState, LongWritable> getGraph(GiraphConfiguration conf) {
        TestGraph<Text, LouvainNodeState, LongWritable> testGraph = new TestGraph<Text, LouvainNodeState, LongWritable>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), new LongWritable(1L));
        return testGraph;
    }
    private TestGraph<Text, LouvainNodeState, LongWritable> getCyclicGraph(GiraphConfiguration conf) {
        TestGraph<Text, LouvainNodeState, LongWritable> testGraph = new TestGraph<Text, LouvainNodeState, LongWritable>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), new LongWritable(1L));
        testGraph.addEdge(new Text("2"), new Text("1"), new LongWritable(1L));
        testGraph.addEdge(new Text("1"), new Text("3"), new LongWritable(1L));
        testGraph.addEdge(new Text("3"), new Text("1"), new LongWritable(1L));
        testGraph.addEdge(new Text("2"), new Text("3"), new LongWritable(1L));
        testGraph.addEdge(new Text("3"), new Text("2"), new LongWritable(1L));
        return testGraph;
    }
    private TestGraph<Text, LouvainNodeState, LongWritable> getBiggerGraph(GiraphConfiguration conf) {
        TestGraph<Text, LouvainNodeState, LongWritable> testGraph = new TestGraph<Text, LouvainNodeState, LongWritable>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), new LongWritable(1L));
        testGraph.addEdge(new Text("1"), new Text("3"), new LongWritable(1L));
        testGraph.addEdge(new Text("1"), new Text("4"), new LongWritable(1L));
        testGraph.addEdge(new Text("1"), new Text("5"), new LongWritable(1L));
        testGraph.addEdge(new Text("1"), new Text("6"), new LongWritable(1L));
        testGraph.addEdge(new Text("1"), new Text("7"), new LongWritable(1L));
        testGraph.addEdge(new Text("1"), new Text("8"), new LongWritable(1L));
        testGraph.addEdge(new Text("1"), new Text("9"), new LongWritable(1L));
        testGraph.addEdge(new Text("9"), new Text("10"), new LongWritable(1L));
        testGraph.addEdge(new Text("9"), new Text("11"), new LongWritable(1L));
        testGraph.addEdge(new Text("9"), new Text("12"), new LongWritable(1L));
        testGraph.addEdge(new Text("9"), new Text("13"), new LongWritable(1L));
        testGraph.addEdge(new Text("9"), new Text("14"), new LongWritable(1L));
        testGraph.addEdge(new Text("9"), new Text("15"), new LongWritable(1L));
        testGraph.addEdge(new Text("9"), new Text("16"), new LongWritable(1L));
        return testGraph;
    }
}
