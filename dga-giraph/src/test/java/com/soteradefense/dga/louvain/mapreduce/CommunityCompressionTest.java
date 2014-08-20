/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

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
