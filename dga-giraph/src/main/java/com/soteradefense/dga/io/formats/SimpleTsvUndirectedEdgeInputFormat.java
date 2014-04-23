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
 * Edge Import Class that parses off of a delimiter set in the config as well as the column number.
 * This also does Reverse Edges to make the graph undirected.
 */
public class SimpleTsvUndirectedEdgeInputFormat extends TextEdgeInputFormat<Text, NullWritable> {

    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    public static final String EXPECTED_NUMBER_OF_COLUMNS_KEY = "simple.tsv.edge.column.count";

    public static final String EXPECTED_NUMBER_OF_COLUMNS = "4";

    /**
     *
     * @param split
     * @param context
     * @return A Reverse Edge Duplicator to make the graph undirected.
     * @throws IOException
     */
    @Override
    public EdgeReader<Text, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new ReverseEdgeDuplicator<Text, NullWritable>(new SimpleTsvEdgeReader());
    }

    /**
     * This reads the delimited file line by line, parses it on the delimiter and returns the Id, Value, etc.
     */
    protected class SimpleTsvEdgeReader extends TextEdgeInputFormat<Text, NullWritable>.TextEdgeReaderFromEachLineProcessed<Text> {
        private String delimiter;
        private NullWritable defaultEdgeWeight;
        private int numberOfExpectedColumns;

        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(inputSplit, context);
            delimiter = context.getConfiguration().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            numberOfExpectedColumns = Integer.parseInt(context.getConfiguration().get(EXPECTED_NUMBER_OF_COLUMNS_KEY, EXPECTED_NUMBER_OF_COLUMNS));
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
            if (splitValues.length != numberOfExpectedColumns)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires " + String.valueOf(numberOfExpectedColumns) + " values.  Data row was [" + value + "]");
            if(splitValues[1].trim().length() == 0)
                throw new IOException("The target vertex is empty! The row is: " + value);
            return new Text(splitValues[1].trim());
        }

        @Override
        protected Text getSourceVertexId(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length != numberOfExpectedColumns)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires " + String.valueOf(numberOfExpectedColumns) + " values.  Data row was [" + value + "]");
            if(splitValues[0].trim().length() == 0)
                throw new IOException("The source vertex is empty! The row is: " + value);
            return new Text(splitValues[0].trim());
        }

        @Override
        protected NullWritable getValue(Text line) throws IOException {
            return defaultEdgeWeight;
        }
    }
}
