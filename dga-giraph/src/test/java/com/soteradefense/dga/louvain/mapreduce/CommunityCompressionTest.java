package com.soteradefense.dga.louvain.mapreduce;


import com.soteradefense.dga.louvain.giraph.LouvainVertexWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CommunityCompressionTest {

    MapDriver<LongWritable, Text, Text, LouvainVertexWritable> mapDriver;
    ReduceDriver<Text, LouvainVertexWritable, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        CommunityCompression.Map mapper = new CommunityCompression.Map();
        mapDriver = MapDriver.newMapDriver(mapper);
        CommunityCompression.Reduce reducer = new CommunityCompression.Reduce();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(), new Text("1\t1\t0\t2:1"));
        LouvainVertexWritable expectedOut = LouvainVertexWritable.fromTokens("0", "2:1");
        mapDriver.withOutput(new Text("1"), expectedOut);
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        List<LouvainVertexWritable> values = new ArrayList<LouvainVertexWritable>();
        values.add(LouvainVertexWritable.fromTokens("0", "2:1"));
        reduceDriver.withInput(new Text("1"), values);
        reduceDriver.withOutput(new Text("1"), new Text("0\t2:1"));

    }
}
