package com.soteradefense.dga.io.formats;

import com.soteradefense.dga.utils.DummyComputation;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SimpleTsvUndirectedEdgeInputFormatTest extends SimpleTsvUndirectedEdgeInputFormat {

    private RecordReader<LongWritable,Text> rr;
    private ImmutableClassesGiraphConfiguration<Text, Text, VIntWritable> conf;
    private TaskAttemptContext tac;

    @Before
    public void setUp() throws IOException, InterruptedException {
        rr = mock(RecordReader.class);
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<Text, Text, VIntWritable>(giraphConf);
        tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);
    }

    public EdgeReader<Text, VIntWritable> createEdgeReader(final RecordReader<LongWritable, Text> rr) throws IOException {
        return new SimpleTsvEdgeReader(){
            @Override
            protected RecordReader<LongWritable, Text> createLineRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
                return rr;
            }

        };
    }

    @Test
    public void test1() throws IOException, InterruptedException {
        String input = "1\t2";
        when(rr.getCurrentValue()).thenReturn(new Text(input));
        EdgeReader ter = createEdgeReader(rr);

        ter.setConf(conf);
        ter.initialize(null, tac);

    }
}