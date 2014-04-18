package com.soteradefense.dga.utils;

import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.RecordReader;


public abstract class DummyRecordReader extends RecordReader<Text, NullWritable> {
}
