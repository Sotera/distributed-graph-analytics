package com.soteradefense.dga.io.formats;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class HDSEEdgeInputFormatTest extends HBSEEdgeInputFormat {
    private RecordReader<LongWritable,Text> rr;
    private ImmutableClassesGiraphConfiguration<Text, Text, VIntWritable> conf;
    private TaskAttemptContext tac;

    @Before
    public void setUp() throws IOException, InterruptedException {
        rr = mock(RecordReader.class);
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(BasicComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<Text, Text, VIntWritable>(giraphConf);
        tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);
    }

    public EdgeReader<IntWritable, IntWritable> createEdgeReader(final RecordReader<LongWritable,Text> rr) throws IOException {
        return new SBEdgeReader(){
            @Override
            protected RecordReader<LongWritable, Text> createLineRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
                return rr;
            }
        };
    }

    @Test
    public void testInputParserWithDefaultWeight() throws IOException, InterruptedException {
        String input = "1,2";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new IntWritable(1));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new IntWritable(2));
        assertEquals(ter.getCurrentEdge().getValue(), new IntWritable(1));

    }

    @Test
    public void testInputParserWithCustomWeight() throws IOException, InterruptedException {
        String input = "1,2,10";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);
        ter.setConf(conf);
        ter.initialize(null, tac);
        assertEquals(ter.getCurrentSourceId(), new IntWritable(1));
        assertEquals(ter.getCurrentEdge().getTargetVertexId(), new IntWritable(2));
        assertEquals(ter.getCurrentEdge().getValue(), new IntWritable(10));

    }
}