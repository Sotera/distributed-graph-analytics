package com.soteradefense.dga.leafcompression;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LeafCompressionInputFormat extends TextVertexInputFormat<Text, VIntWritable, Text> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new KeyDataVertexReader();
    }

    /**
     * Reads the input and converts each line to a vertex.
     * Creates a link from each vertex to the previous one for communication during the computation.
     */
    public class KeyDataVertexReader extends TextVertexInputFormat<Text, VIntWritable,Text>.TextVertexReader {

        @Override
        public Vertex<Text, VIntWritable, Text> getCurrentVertex() throws IOException, InterruptedException {
            Vertex<Text, VIntWritable, Text> vertex;
            String line = getRecordReader().getCurrentValue().toString();
            //Parse each line and create the vertex and edges
            String[] tokens = line.toString().trim().split("\t");
            if (tokens.length == 1) {
                //Null sender on the left hand side
                vertex = null;
                throw new IllegalArgumentException("bad data in line: " + line);
            }
            else if (tokens.length != 2) {
                throw new IllegalArgumentException("bad arguments in line: " + line);
            }
            else {
                vertex = getConf().createVertex();
                List<Edge<Text, Text>> edges = edgesFrom(tokens);
                Text vertexId = new Text(tokens[0]);
                VIntWritable vertexValue = new VIntWritable(0);
                vertex.initialize(vertexId, vertexValue, edges);
            }

            return vertex;
        }

        private List<Edge<Text, Text>> edgesFrom(String[] edgeArray) {
            List<Edge<Text, Text>> edges = new ArrayList<Edge<Text,Text>>();
            String[] destinationList = edgeArray[1].trim().split(",");
            for (String destination : destinationList) {
                Text edgeVertex = new Text(destination.trim());
                Text edgeValue = new Text(destination.trim());
                edges.add(EdgeFactory.create(edgeVertex, edgeValue));
            }

            return edges;
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }
    }

}