package com.soteradefense.dga.io.formats;


import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 *
 */
public class SimpleTsvEdgeInputFormat extends TextEdgeInputFormat<Text, VIntWritable> {

    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    public static final String EDGE_WEIGHT_VALUE = "simple.tsv.edge.weight.default";

    public static final String EDGE_WEIGHT_VALUE_DEFAULT = "1";

    @Override
    public EdgeReader<Text, VIntWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new SimpleTsvEdgeReader();
    }

    protected class SimpleTsvEdgeReader extends TextEdgeInputFormat<Text, VIntWritable>.TextEdgeReaderFromEachLineProcessed<Text> {
        private String delimiter;
        private VIntWritable defaultEdgeWeight;

        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(inputSplit, context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            String edgeWeight = getConf().get(EDGE_WEIGHT_VALUE, EDGE_WEIGHT_VALUE_DEFAULT);

            try {
                defaultEdgeWeight = new VIntWritable(Integer.parseInt(edgeWeight));
            } catch (NumberFormatException e) {
                throw new IOException("The default edge weight provided (configuration parameter: " + EDGE_WEIGHT_VALUE +", value: " + edgeWeight + ", could not be cast to integer.");
            }

        }

        @Override
        protected Text preprocessLine(Text line) throws IOException {
            return new Text(line.toString().trim());
        }

        @Override
        protected Text getTargetVertexId(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length != 2 && splitValues.length != 3)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires 2 or 3 values.  Data row was [" + value + "]");
            return new Text(splitValues[1].trim());
        }

        @Override
        protected Text getSourceVertexId(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length != 2 && splitValues.length != 3)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires 2 or 3 values.  Data row was [" + value + "]");
            return new Text(splitValues[0].trim());
        }

        @Override
        protected VIntWritable getValue(Text line) throws IOException {
            String value = line.toString();
            String splitValues[] = value.split(delimiter);
            if (splitValues.length != 2 && splitValues.length != 3)
                throw new IOException("Row of data, after tokenized based on delimiter [ " + delimiter + "], had " + splitValues.length + " tokens, but this format requires 2 or 3 values.  Data row was [" + value + "]");
            if (splitValues.length == 2){
                return defaultEdgeWeight;
            }
            try {
                return new VIntWritable(Integer.parseInt(splitValues[2].trim()));
            } catch (NumberFormatException e) {
                throw new IOException("Row of data contained an invalid edge weight, [" + splitValues[2].trim() + "]");
            }
        }
    }
}
