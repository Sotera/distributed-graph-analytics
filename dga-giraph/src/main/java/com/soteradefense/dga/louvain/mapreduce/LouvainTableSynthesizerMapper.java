package com.soteradefense.dga.louvain.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LouvainTableSynthesizerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(LouvainTableSynthesizerMapper.class);
    private static final String GIRAPH_0 = LouvainTableSynthesizer.GIRAPH_FOLDER_BASE_NAME + LouvainTableSynthesizer.FILE_NAME_SEPARATOR + "0";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String prepend = value.toString().trim();
        String[] tokens = prepend.split("\t");
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getParent().getName();
        String groupBy;
        StringBuilder builder = new StringBuilder();
        if (fileName.contains(GIRAPH_0)) {
            //Right Side Join
            groupBy = tokens[1];
            builder.append(tokens[0]);
            builder.append('\t');
            builder.append(tokens[1]);
            builder.append(':');
            builder.append("0");
        } else if (fileName.contains(LouvainTableSynthesizer.TABLE_BASE_NAME)) {
            //Right Side Join
            groupBy = tokens[tokens.length - 1];
            builder.append(prepend);
            builder.append(':');
            builder.append("0");
        } else {
            //Left Side Join
            groupBy = tokens[0];
            builder.append(tokens[1]);
            builder.append(':');
            builder.append("1");
        }
        Text outKey = new Text(groupBy.trim());
        context.write(outKey, new Text(builder.toString()));
    }

}
