package com.soteradefense.dga.io.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * This returns a TextVertexWriter to output the results of a vertex to a file.
 */
public class SimpleTsvVertexOutput extends TextVertexOutputFormat<Text,Text,NullWritable> {

    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.vertex.delimiter";

    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new VertexOutput();
    }
    /**
     * This is a Vertex Output that outputs the vertex id and value between a specified delimiter.
     */
    protected class VertexOutput extends TextVertexWriterToEachLine{

        private String delimiter;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
        }

        @Override
        protected Text convertVertexToLine(Vertex<Text, Text, NullWritable> vertex) throws IOException {
            return new Text(vertex.getId() + delimiter + vertex.getValue());
        }
    }
}
