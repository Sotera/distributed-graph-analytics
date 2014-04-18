package com.soteradefense.dga.io.formats;


import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 *
 */
public class SimpleTsvUndirectedEdgeInputFormat extends TextEdgeInputFormat<Text, NullWritable> {

    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    public static final int MIN_NUMBER_OF_COLUMNS = 2;

    @Override
    public EdgeReader<Text, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new ReverseEdgeDuplicator<Text, NullWritable>(new SimpleTsvEdgeReader());
    }

    protected class SimpleTsvEdgeReader extends TextEdgeInputFormat<Text, NullWritable>.TextEdgeReaderFromEachLineProcessed<Text> {
        private String delimiter;
        private NullWritable defaultEdgeWeight;

        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(inputSplit, context);
            //System.out.println(context.getConfiguration() != null);
            //System.out.println(getConf() != null);
            delimiter = context.getConfiguration().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            defaultEdgeWeight = NullWritable.get();
        }

        @Override
        protected Text preprocessLine(Text line) throws IOException {
            return new Text(line.toString().trim());
        }

        @Override
        protected Text getTargetVertexId(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length < MIN_NUMBER_OF_COLUMNS)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires 4 values.  Data row was [" + value + "]");
            return new Text(splitValues[1].trim());
        }

        @Override
        protected Text getSourceVertexId(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length < MIN_NUMBER_OF_COLUMNS)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires 4 values.  Data row was [" + value + "]");
            return new Text(splitValues[0].trim());
        }

        @Override
        protected NullWritable getValue(Text line) throws IOException {
            return defaultEdgeWeight;
        }
    }
}
