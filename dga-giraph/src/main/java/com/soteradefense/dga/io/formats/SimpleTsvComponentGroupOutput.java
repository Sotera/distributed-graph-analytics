package com.soteradefense.dga.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextEdgeOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * This is a class that returns a TextEdgeWriter that outputs each edge line by line.
 */
public class SimpleTsvComponentGroupOutput extends TextEdgeOutputFormat<Text, Text, NullWritable> {

    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    @Override
    public TextEdgeWriter<Text, Text, NullWritable> createEdgeWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new IdGroupWriter();
    }
    /**
     * This class outputs the edges and another column which is the component it belongs to.
     */
    protected class IdGroupWriter extends TextEdgeWriterToEachLine<Text,Text,NullWritable>{
        private String delimiter;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
        }

        @Override
        protected Text convertEdgeToLine(Text sourceId, Text sourceValue, Edge<Text, NullWritable> edge) throws IOException {
            return new Text(sourceId.toString().trim() + delimiter + edge.getTargetVertexId().toString().trim() + delimiter + sourceValue.toString().trim());
        }
    }
}
