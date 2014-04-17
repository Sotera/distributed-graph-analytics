package com.soteradefense.dga.utils;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

public abstract class DummyEdge implements Edge<Text, VIntWritable> {

}
