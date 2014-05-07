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
        conf.setComputationClass(HBSEVertexComputation.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
        conf.set("betweenness.output.dir", "tmp/output");
        conf.set("betweenness.set.stability", "1");
        conf.set("betweenness.set.maxSize", "10");
        conf.set("betweenness.set.stability.counter", "3");
        conf.set("pivot.batch.string", "1,2,3,4,5");
        conf.set("pivot.batch.size", "5");
        conf.set("vertex.count", "7");
        return conf;
    }

    @Test
    public void testComputeOutput() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<IntWritable, VertexData, IntWritable> input = getGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<IntWritable, VertexData, IntWritable> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertEquals(output.getVertex(new IntWritable(2)).getValue().getApproxBetweenness() > 0.0, true);
        assertEquals(output.getVertex(new IntWritable(1)).getValue().getApproxBetweenness() > 0.0, true);
    }

    private TestGraph<IntWritable, VertexData, IntWritable> getGraph(GiraphConfiguration conf) {
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
