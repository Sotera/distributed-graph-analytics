package com.soteradefense.dga.leafcompression.com.soteradefense.dga.io.formats;


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
public class SimpleTsvEdgeFormat extends TextEdgeInputFormat<Text, VIntWritable> {

    public static final String LINE_TOKENIZE_VALUE = "simple.tsv.edge.delimiter";

    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    public static final String EDGE_WEIGHT_VALUE = "simple.tsv.edge.weight.default";

    public static final String EDGE_WEIGHT_VALUE_DEFAULT = "1";

    @Override
    public EdgeReader<Text, VIntWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new SimpleTsvEdgeReader();
    }



    private class SimpleTsvEdgeReader extends TextEdgeInputFormat<Text, VIntWritable>.TextEdgeReader {

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
        public boolean nextEdge() throws IOException, InterruptedException {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Text getCurrentSourceId() throws IOException, InterruptedException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Edge<Text, VIntWritable> getCurrentEdge() throws IOException, InterruptedException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
