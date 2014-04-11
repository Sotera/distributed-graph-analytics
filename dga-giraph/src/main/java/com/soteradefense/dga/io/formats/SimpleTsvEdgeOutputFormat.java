package com.soteradefense.dga.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextEdgeOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class SimpleTsvEdgeOutputFormat extends TextEdgeOutputFormat<Text, VIntWritable, VIntWritable> {

    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    @Override
    public TextEdgeWriter<Text, VIntWritable, VIntWritable> createEdgeWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new TextEdgeWriterToEachLine<Text, VIntWritable, VIntWritable>() {
            private String delimiter;
            @Override
            public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
                super.initialize(context);
                delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            }

            @Override
            protected Text convertEdgeToLine(Text sourceId, VIntWritable sourceValue, Edge<Text, VIntWritable> edge) throws IOException {
                return new Text(sourceId + delimiter + edge.getTargetVertexId() + delimiter + edge.getValue().toString());
            }
        };
    }
}
