package com.soteradefense.dga.io.formats;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
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


public class SimpleTsvComponentGroupOutputTest extends SimpleTsvComponentGroupOutput {

    /**
     * Test configuration
     */
    private ImmutableClassesGiraphConfiguration<
            Text, Text, NullWritable> conf;

    @Override
    public TextEdgeWriter<Text, Text, NullWritable> createEdgeWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        super.createEdgeWriter(context);
        final TextEdgeWriter<Text, Text, NullWritable> tw = mock(TextEdgeWriter.class);
        return tw;
    }



    @Before
    public void setUp() throws Exception {
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
        giraphConfiguration.setComputationClass(BasicComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<Text, Text,
                NullWritable>(giraphConfiguration);
    }

    @Test
    public void testValidOutput() throws IOException, InterruptedException {
        Text expected = new Text("email@email.com\tanother@email.com\temail@email.com");
        testWriter(expected);
    }

    @Test
    public void testNewDelimiter() throws IOException, InterruptedException {
        conf.set(LINE_TOKENIZE_VALUE, "\n");
        Text expected = new Text("email@email.com\nanother@email.com\nemail@email.com");
        testWriter(expected);
    }

    private void testWriter(Text expected) throws IOException, InterruptedException {
        TaskAttemptContext tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);

        Vertex<Text,Text,NullWritable> vertex = mock(Vertex.class);
        when(vertex.getId()).thenReturn(new Text("email@email.com"));
        when(vertex.getValue()).thenReturn(new Text("email@email.com"));
        Edge<Text,NullWritable> edge = mock(Edge.class);
        when(edge.getTargetVertexId()).thenReturn(new Text("another@email.com"));
        // Create empty iterator == no edges
        final RecordWriter<Text,Text> tw = mock(RecordWriter.class);
        IdGroupWriter writer = new IdGroupWriter() {
            @Override
            protected RecordWriter<Text, Text> createLineRecordWriter(
                    TaskAttemptContext context) throws IOException, InterruptedException {
                return tw;
            }
        };
        writer.setConf(conf);
        writer.initialize(tac);
        writer.writeEdge(vertex.getId(), vertex.getValue(), edge);
        verify(tw).write(expected, null);
    }


}
