package com.soteradefense.dga.louvain.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LouvainTableSynthesizerReducer extends Reducer<Text, Text, Text, NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(LouvainTableSynthesizerReducer.class);

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        String rightText = null;
        List<String> zeros = new ArrayList<String>();
        for (Text item : values) {
            String valueItem = item.toString();
            String[] tokens = valueItem.split(":");
            int side = Integer.parseInt(tokens[1]);
            if (side == 1) {
                rightText = tokens[0];
            } else {
                zeros.add(tokens[0]);
            }
        }
        for (String item : zeros) {
            builder.append(item);
            if (rightText != null) {
                builder.append('\t');
                builder.append(rightText);
            }
            context.write(new Text(builder.toString()), NullWritable.get());
            builder.setLength(0);
        }

    }
}
