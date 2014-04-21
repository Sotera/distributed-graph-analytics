package com.soteradefense.dga.weaklyconnectedcomponents;


import com.google.common.collect.Maps;
import com.soteradefense.dga.io.formats.SimpleTsvComponentGroupOutput;
import com.soteradefense.dga.io.formats.SimpleTsvUndirectedEdgeInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
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
        String[] data = {
                "1\t2",
                "3\t4",
                "5\t6"
        };
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(WeaklyConnectedComponents.class);
        conf.setEdgeInputFormatClass(SimpleTsvUndirectedEdgeInputFormat.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
        conf.set(SimpleTsvUndirectedEdgeInputFormat.EXPECTED_NUMBER_OF_COLUMNS_KEY, "2");
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, null, data);
        TestGraph<Text, Text, NullWritable> graph = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(6, graph.getVertices().size());
        assertEquals("2", graph.getVertex(new Text("1")).getValue().toString());
        assertEquals("2", graph.getVertex(new Text("2")).getValue().toString());
        assertEquals("4", graph.getVertex(new Text("3")).getValue().toString());
        assertEquals("4", graph.getVertex(new Text("4")).getValue().toString());
        assertEquals("6", graph.getVertex(new Text("5")).getValue().toString());
        assertEquals("6", graph.getVertex(new Text("6")).getValue().toString());
    }

}
