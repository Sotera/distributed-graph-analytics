package com.soteradefense.dga.leafcompression;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class LeafCompressionComputation extends BasicComputation<Text, VIntWritable, Text, Text> {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GiraphRunner(), args));
    }

    @Override
    public void compute(Vertex<Text, VIntWritable, Text> vertex, Iterable<Text> messages) throws IOException {
        try {
            // Check to see if we received any messages from connected nodes notifying us
            // that they have only a single edge, and can subsequently be pruned from the graph
            for (Text incomingMessage : messages) {
                Text messageVertex = new Text(incomingMessage.toString().split(":")[0]);
                int value = Integer.parseInt(incomingMessage.toString().split(":")[1]);
                vertex.setValue(new VIntWritable(vertex.getValue().get() + 1 + value));

                // Remove the vertex and its corresponding edge
                removeVertexRequest(messageVertex);
                vertex.removeEdges(messageVertex);
            }

            // Broadcast the edges if we only have a single edge
            sendEdges(vertex);
        } catch (Exception e) {
            System.err.println(e.toString());
        }
    }

    private void sendEdges(Vertex<Text, VIntWritable, Text> vertex) {
        if (vertex.getNumEdges() == 1 && vertex.getValue().get() != -1) {
            for (Edge<Text, Text> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), new Text(vertex.getId().toString() + ":" + vertex.getValue().toString()));
            }
            vertex.setValue(new VIntWritable(-1));
            // This node will never vote to halt, but will simply be deleted.
        } else if (vertex.getValue().get() != -1) {
            // If we aren't being imminently deleted
            vertex.voteToHalt();
        }
    }

}