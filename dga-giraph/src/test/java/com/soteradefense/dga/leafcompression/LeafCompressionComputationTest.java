package com.soteradefense.dga.leafcompression;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.utils.InMemoryVertexInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class LeafCompressionComputationTest {

    private GiraphConfiguration giraphConf;

    @Before
    public void setUp() {
        giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(LeafCompressionComputation.class);
        giraphConf.setVertexInputFormatClass(InMemoryVertexInputFormat.class);
        giraphConf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
    }

    @Test
    public void testComputationGoodData() throws Exception {
        TestGraph<Text, Text, Text> testGraph = new TestGraph(giraphConf);
        testGraph.addEdge(new Text("1"), new Text("2"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("3"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("4"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("5"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("6"), new Text("1"));
        testGraph.addEdge(new Text("2"), new Text("3"), new Text("1"));
        testGraph.addEdge(new Text("2"), new Text("4"), new Text("1"));
        testGraph.addEdge(new Text("2"), new Text("5"), new Text("1"));
        testGraph.addEdge(new Text("2"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("3"), new Text("4"), new Text("1"));
        testGraph.addEdge(new Text("3"), new Text("5"), new Text("1"));
        testGraph.addEdge(new Text("3"), new Text("20"), new Text("1"));
        testGraph.addEdge(new Text("3"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("3"), new Text("2"), new Text("1"));
        testGraph.addEdge(new Text("4"), new Text("5"), new Text("1"));
        testGraph.addEdge(new Text("4"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("4"), new Text("2"), new Text("1"));
        testGraph.addEdge(new Text("4"), new Text("3"), new Text("1"));
        testGraph.addEdge(new Text("5"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("5"), new Text("2"), new Text("1"));
        testGraph.addEdge(new Text("5"), new Text("3"), new Text("1"));
        testGraph.addEdge(new Text("5"), new Text("4"), new Text("1"));
        testGraph.addEdge(new Text("6"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("6"), new Text("8"), new Text("1"));
        testGraph.addEdge(new Text("8"), new Text("6"), new Text("1"));
        testGraph.addEdge(new Text("8"), new Text("10"), new Text("1"));
        testGraph.addEdge(new Text("10"), new Text("8"), new Text("1"));
        testGraph.addEdge(new Text("10"), new Text("12"), new Text("1"));
        testGraph.addEdge(new Text("12"), new Text("10"), new Text("1"));
        testGraph.addEdge(new Text("15"), new Text("17"), new Text("1"));
        testGraph.addEdge(new Text("15"), new Text("19"), new Text("1"));
        testGraph.addEdge(new Text("17"), new Text("15"), new Text("1"));
        testGraph.addEdge(new Text("19"), new Text("15"), new Text("1"));
        testGraph.addEdge(new Text("19"), new Text("20"), new Text("1"));
        testGraph.addEdge(new Text("20"), new Text("3"), new Text("1"));
        testGraph.addEdge(new Text("20"), new Text("19"), new Text("1"));
        ImmutableClassesGiraphConfiguration conf = new ImmutableClassesGiraphConfiguration(giraphConf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, testGraph);
        TestGraph<Text, Text, Text> outputGraph = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(5, outputGraph.getVertices().size());
        checkConnectionsFromVertex(outputGraph, "1", "2", "3", "4", "5");
        checkConnectionsFromVertex(outputGraph, "2", "3", "4", "5", "1");
        checkConnectionsFromVertex(outputGraph, "3", "4", "5", "1", "2");
        checkConnectionsFromVertex(outputGraph, "4", "5", "1", "2", "3");
        checkConnectionsFromVertex(outputGraph, "5", "1", "2", "3", "4");
    }

    /*
        This lookup is built from all of the edges in a vertex - it allows us to reference them by the
        Edge#targetVertexId(), as a string, without converting repeatedly elsewhere
     */
    private Map<String, Edge<Text, Text>> buildLookup(Vertex<Text, Text, Text> vertex) {
        Map<String, Edge<Text, Text>> lookup = new HashMap<String, Edge<Text, Text>>();
        for(Edge<Text, Text> edge : vertex.getEdges()) {
            lookup.put(edge.getTargetVertexId().toString(), edge);
        }
        return lookup;
    }

    /*
        Method takes in the TestGraph, the Vertex ID we're testing connections for (as well as ensuring that Vertex even
        exists in our TestGraph, and a varargs list of Strings that will represent the Edge.targetVertexId we want to
        find matches for.

        As order is not guaranteed, we use a hashmap for lookups.
     */
    private void checkConnectionsFromVertex(TestGraph graph, String vertex, String ... edges) {
        Text vertexText = new Text(vertex);
        Vertex<Text, Text, Text> vert = graph.getVertex(vertexText);
        assertNotNull("Vertex " + vertex + " was null!", vert);
        Map<String, Edge<Text, Text>> existingEdges = buildLookup(vert);
        for (String node : edges) {
            assertNotNull("Node " + node + " was not found", existingEdges.get(node));
        }
    }

    @Test
    public void testComputationUnconnectedGraph() throws Exception {
        TestGraph<Text, Text, Text> testGraph = new TestGraph(giraphConf);
        ImmutableClassesGiraphConfiguration conf = new ImmutableClassesGiraphConfiguration(giraphConf);
        testGraph.addEdge(new Text("1"), new Text("2"), new Text("1"));
        testGraph.addEdge(new Text("4"), new Text("3"), new Text("1"));
        testGraph.addEdge(new Text("28"), new Text("16"), new Text("1"));

        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, testGraph);
        TestGraph<Text, Text, Text> outputGraph = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(3, outputGraph.getVertices().size());
        assertNull(outputGraph.getVertex(new Text("1")));
        assertNotNull(outputGraph.getVertex(new Text("2")));
        assertEquals(0, outputGraph.getVertex(new Text("2")).getNumEdges());
        assertNull(outputGraph.getVertex(new Text("4")));
        assertNotNull(outputGraph.getVertex(new Text("3")));
        assertEquals(0, outputGraph.getVertex(new Text("3")).getNumEdges());
        assertNull(outputGraph.getVertex(new Text("28")));
        assertNotNull(outputGraph.getVertex(new Text("16")));
        assertEquals(0, outputGraph.getVertex(new Text("16")).getNumEdges());
    }

    @Test
    public void testActualTextNodes() throws Exception {
        TestGraph<Text, Text, Text> testGraph = new TestGraph(giraphConf);
        ImmutableClassesGiraphConfiguration conf = new ImmutableClassesGiraphConfiguration(giraphConf);
        testGraph.addEdge(new Text("Jack"), new Text("Jill"), new Text("1"));
        testGraph.addEdge(new Text("Jill"), new Text("Jack"), new Text("1"));
        testGraph.addEdge(new Text("Jane"), new Text("Jill"), new Text("1"));
        testGraph.addEdge(new Text("Jill"), new Text("Jane"), new Text("1"));
        testGraph.addEdge(new Text("Jack"), new Text("Jane"), new Text("1"));
        testGraph.addEdge(new Text("Jane"), new Text("Jack"), new Text("1"));
        testGraph.addEdge(new Text("Jane"), new Text("Gregory"), new Text("1"));
        testGraph.addEdge(new Text("Gregory"), new Text("Straxx"), new Text("1"));

        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, testGraph);
        TestGraph<Text, Text, Text> outputGraph = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(4, outputGraph.getVertices().size());
        checkConnectionsFromVertex(outputGraph, "Jack", "Jane", "Jill");
        checkConnectionsFromVertex(outputGraph, "Jane", "Jack", "Jill");
        checkConnectionsFromVertex(outputGraph, "Jill", "Jack", "Jane");
        assertEquals(0, outputGraph.getVertex(new Text("Straxx")).getNumEdges());
    }

}
