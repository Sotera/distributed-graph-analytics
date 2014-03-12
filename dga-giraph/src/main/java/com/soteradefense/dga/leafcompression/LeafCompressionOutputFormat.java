package com.soteradefense.dga.leafcompression;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LeafCompressionOutputFormat extends TextVertexOutputFormat<Text, VIntWritable, Text> {

    /**
     * Reads the input and converts each line to a vertex.
     * Creates a link from each vertex to the previous one for communication during the computation.
     */
    private class LeafCompressionVertexWriter extends TextVertexOutputFormat<Text, VIntWritable, Text>.TextVertexWriter {

        public void writeVertex(Vertex<Text, VIntWritable, Text> vertex)
                throws IOException, InterruptedException {
            if (!vertex.getValue().toString().isEmpty()) {
                getRecordWriter().write(new Text(vertex.getId().toString()), new Text(vertex.getValue().toString()));
            }

        }
    }

    @Override
    public TextVertexOutputFormat.TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new LeafCompressionVertexWriter();
    }

}
