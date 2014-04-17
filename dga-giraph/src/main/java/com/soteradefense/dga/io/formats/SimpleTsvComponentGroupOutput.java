package com.soteradefense.dga.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextEdgeOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

public class SimpleTsvComponentGroupOutput extends TextEdgeOutputFormat<Text, Text, VIntWritable> {

    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    @Override
    public TextEdgeWriter<Text, Text, VIntWritable> createEdgeWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new IdGroupWriter();
    }
    protected class IdGroupWriter extends TextEdgeWriterToEachLine<Text,Text,VIntWritable>{
        private String delimiter;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
        }

        @Override
        protected Text convertEdgeToLine(Text sourceId, Text sourceValue, Edge<Text, VIntWritable> edge) throws IOException {
            return new Text(sourceId + delimiter + edge.getTargetVertexId() + delimiter + sourceValue);
        }
    }
}
