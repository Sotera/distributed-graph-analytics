package com.soteradefense.dga.io.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * Created by ekimbrel on 8/21/15.
 */



public class IntVertexOutputFormat extends TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {

    @Override
    public TextVertexOutputFormat.TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new SimpleVertexWriter();
    }

    /**
     * A simple vertex writer that writes the VertexId and Vertex Value
     */
    public class SimpleVertexWriter extends TextVertexWriter {

        /**
         * Writes a Vertex
         *
         * @param vertex Vertex to Write
         * @throws IOException
         * @throws InterruptedException
         */
        public void writeVertex(Vertex<IntWritable, IntWritable, NullWritable> vertex) throws IOException, InterruptedException {
            getRecordWriter().write(new Text(vertex.getId().toString()), new Text(vertex.getValue().toString()));
        }

    }

}
