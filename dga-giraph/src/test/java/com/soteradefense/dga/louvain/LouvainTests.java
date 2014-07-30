package com.soteradefense.dga.louvain;

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
        conf.set("mapred.output.dir", "tmp/output_0");
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

    private TestGraph<Text, LouvainNodeState, LongWritable> getGraph(GiraphConfiguration conf) {
        TestGraph<Text, LouvainNodeState, LongWritable> testGraph = new TestGraph<Text, LouvainNodeState, LongWritable>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), new LongWritable(1L));
        return testGraph;
    }
}
