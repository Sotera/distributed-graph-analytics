package com.soteradefense.dga.highbetweenness;

import com.soteradefense.dga.io.formats.HBSEEdgeInputFormat;
import com.soteradefense.dga.io.formats.HBSEOutputFormat;
import com.soteradefense.dga.io.formats.SimpleTsvUndirectedEdgeInputFormat;
import com.soteradefense.dga.weaklyconnectedcomponents.WeaklyConnectedComponentCompute;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.python.google.common.collect.Multiset;

import java.util.AbstractMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * Created by jostinowsky on 2014-05-01.
 */
public class HBSEComputeTest {
    public GiraphConfiguration getConf(){
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(HBSEMasterCompute.class);
        conf.setComputationClass(HBSEVertexComputation.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);

        //conf.setEdgeInputFormatClass(HBSEEdgeInputFormat.class);

        conf.set("betweenness.output.dir", "tmp/output");
        conf.set("betweenness.set.stability", "1");
        conf.set("betweenness.set.maxSize", "10");
        conf.set("betweenness.set.stability.counter", "3");
        //conf.set("pivot.batch.string", "1,2,3,4,5");
        conf.set("pivot.batch.size", "5");
        conf.set("vertex.count", "7");
        return conf;
    }
    @Test
    public void testComputeOutput() throws Exception {
        String [] data = {"1,2", "1,3", "1,4", "1,5", "1,6", "1,7", "2,8"};
        GiraphConfiguration conf = getConf();
        TestGraph<IntWritable, VertexData, IntWritable> input = getGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        // Working InternalVertexRunner.run(conf, null, data);
        InternalVertexRunner.run(conf, input);
        TestGraph<IntWritable, VertexData, IntWritable> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertEquals(output.getVertex(new IntWritable(2)).getValue().getApproxBetweenness() > 0.0, true);
    }
    private TestGraph<IntWritable, VertexData, IntWritable> getGraph(GiraphConfiguration conf){
        TestGraph<IntWritable, VertexData, IntWritable> testGraph = new TestGraph<IntWritable, VertexData, IntWritable>(conf);
        testGraph.addEdge(new IntWritable(1), new IntWritable(2), new IntWritable(1));
        testGraph.addEdge(new IntWritable(1), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(2), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(3), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(4), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(5), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(6), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(7), new IntWritable(1), new IntWritable(1));
        testGraph.addEdge(new IntWritable(2), new IntWritable(8), new IntWritable(1));
        initializeVertex(testGraph.getVertex(new IntWritable(1)));
        initializeVertex(testGraph.getVertex(new IntWritable(2)));
        initializeVertex(testGraph.getVertex(new IntWritable(3)));
        initializeVertex(testGraph.getVertex(new IntWritable(4)));
        initializeVertex(testGraph.getVertex(new IntWritable(5)));
        initializeVertex(testGraph.getVertex(new IntWritable(6)));
        initializeVertex(testGraph.getVertex(new IntWritable(7)));
        return testGraph;
    }
    private void initializeVertex(Vertex<IntWritable, VertexData, IntWritable> v){
        v.setValue(new VertexData());
    }
}
