package com.soteradefense.dga.utils;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

import java.io.IOException;

public class DummyComputation extends BasicComputation<Text, Text, VIntWritable, Text> {
    @Override
    public void compute(Vertex<Text, Text, VIntWritable> vertex, Iterable<Text> messages) throws IOException {
        vertex.voteToHalt();
    }
}