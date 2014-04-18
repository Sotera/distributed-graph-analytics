package com.soteradefense.dga.utils;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

public abstract class DummyVertex implements Vertex<Text, Text, NullWritable> {

}
