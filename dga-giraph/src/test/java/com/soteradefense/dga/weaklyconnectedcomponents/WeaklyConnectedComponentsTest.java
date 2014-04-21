package com.soteradefense.dga.weaklyconnectedcomponents;


import com.soteradefense.dga.io.formats.SimpleTsvUndirectedEdgeInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.utils.InMemoryVertexInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;


import java.util.Map;

import static junit.framework.Assert.assertEquals;

public class WeaklyConnectedComponentsTest {

    @Test
    public void testComputeOutput() throws Exception {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(WeaklyConnectedComponents.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
        conf.set(SimpleTsvUndirectedEdgeInputFormat.EXPECTED_NUMBER_OF_COLUMNS_KEY, "2");
        TestGraph<Text,Text,NullWritable> testGraph = getGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, testGraph);
        TestGraph<Text, Text, NullWritable> graph = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(6, graph.getVertices().size());
        assertEquals("2", graph.getVertex(new Text("1")).getValue().toString());
        assertEquals("2", graph.getVertex(new Text("2")).getValue().toString());
        assertEquals("4", graph.getVertex(new Text("3")).getValue().toString());
        assertEquals("4", graph.getVertex(new Text("4")).getValue().toString());
        assertEquals("6", graph.getVertex(new Text("5")).getValue().toString());
        assertEquals("6", graph.getVertex(new Text("6")).getValue().toString());
    }
    private TestGraph<Text,Text,NullWritable> getGraph(GiraphConfiguration conf){
        Text defaultVal = new Text("");
        TestGraph<Text, Text, NullWritable> testGraph = new TestGraph<Text, Text, NullWritable>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), NullWritable.get());
        testGraph.addEdge(new Text("2"), new Text("1"), NullWritable.get());
        testGraph.addEdge(new Text("3"), new Text("4"), NullWritable.get());
        testGraph.addEdge(new Text("4"), new Text("3"), NullWritable.get());
        testGraph.addEdge(new Text("5"), new Text("6"), NullWritable.get());
        testGraph.addEdge(new Text("6"), new Text("5"), NullWritable.get());
        return testGraph;
    }
}
