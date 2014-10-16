package com.soteradefense.dga.io.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DGAVertexOutputFormat extends TextVertexOutputFormat<Text, DoubleWritable, Text> {

    @Override
    public TextVertexOutputFormat.TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new SimpleVertexWriter();
    }

    /**
     * A simple vertex writer that writes the Vertex and it's HighBetweenness Value.
     */
    public class SimpleVertexWriter extends TextVertexWriter {

        /**
         * Writes a Vertex
         *
         * @param vertex Vertex to Write
         * @throws IOException
         * @throws InterruptedException
         */
        public void writeVertex(Vertex<Text, DoubleWritable, Text> vertex) throws IOException, InterruptedException {
            getRecordWriter().write(new Text(vertex.getId().toString()), new Text(vertex.getValue().toString()));
        }

    }

}
