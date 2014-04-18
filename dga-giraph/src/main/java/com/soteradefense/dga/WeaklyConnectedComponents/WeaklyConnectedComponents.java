package com.soteradefense.dga.weaklyconnectedcomponents;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class WeaklyConnectedComponents extends BasicComputation<Text, Text, NullWritable, Text> {

    public static final String DEFAULT_VALUE_VERTEX = "";

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GiraphRunner(), args));
    }

    @Override
    public void compute(Vertex<Text, Text, NullWritable> vertex, Iterable<Text> messages) throws IOException {
        try {
            boolean changed = false;
            String maxId = vertex.getValue().toString();
            if(maxId.equals(DEFAULT_VALUE_VERTEX)){
                maxId = vertex.getId().toString();
                changed = true;
            }
            for (Text incomingMessage : messages) {
                if(maxId.compareTo(incomingMessage.toString()) < 0){
                    maxId = incomingMessage.toString();
                    changed = true;
                }
            }
            vertex.setValue(new Text(maxId));
            broadcastUpdates(vertex, changed);
        } catch (Exception e) {
            System.err.print(e.toString());
        }
    }

    private void broadcastUpdates(Vertex<Text,Text,NullWritable> vertex, boolean changed){
        if(changed) {
            for (Edge<Text, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), new Text(vertex.getValue().toString()));
            }
        }
        vertex.voteToHalt();
    }
}
