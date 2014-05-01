package com.soteradefense.dga.io.formats;

import com.soteradefense.dga.highbetweenness.VertexData;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextEdgeOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class HBSEOutputFormatTest extends HBSEOutputFormat {
    private ImmutableClassesGiraphConfiguration<
            IntWritable,VertexData,IntWritable> conf;
    @Before
    public void setUp() throws Exception {
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
        giraphConfiguration.setComputationClass(BasicComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<IntWritable,VertexData,IntWritable>(giraphConfiguration);
    }
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        super.createVertexWriter(context);
        final TextVertexWriter vw = mock(TextVertexWriter.class);
        return vw;
    }

    @Test
    public void testNewDelimiter() throws IOException, InterruptedException {
        Text expected = new Text("1");
        Text approx = new Text("0.0");
        testWriter(expected, approx);
    }

    private void testWriter(Text expected, Text approx) throws IOException, InterruptedException {
        TaskAttemptContext tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);

        Vertex<IntWritable,VertexData,IntWritable> vertex = mock(Vertex.class);
        when(vertex.getId()).thenReturn(new IntWritable(1));
        when(vertex.getValue()).thenReturn(new VertexData());
        // Create empty iterator == no edges
        final RecordWriter<Text,Text> tw = mock(RecordWriter.class);
        SBVertexWriter writer = new SBVertexWriter() {
            @Override
            protected RecordWriter<Text, Text> createLineRecordWriter(
                    TaskAttemptContext context) throws IOException, InterruptedException {
                return tw;
            }
        };
        writer.setConf(conf);
        writer.initialize(tac);
        writer.writeVertex(vertex);
        verify(tw).write(expected, approx);
    }
}
