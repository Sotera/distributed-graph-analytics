package com.soteradefense.dga.io.formats;

import com.soteradefense.dga.utils.DummyComputation;
import com.soteradefense.dga.utils.DummyEdge;
import com.soteradefense.dga.utils.DummyRecordWriter;
import com.soteradefense.dga.utils.DummyVertex;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
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
            Text, Text, VIntWritable> conf;

    @Override
    public TextEdgeWriter<Text, Text, VIntWritable> createEdgeWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        super.createEdgeWriter(context);
        final TextEdgeWriter<Text, Text, VIntWritable> tw = mock(TextEdgeWriter.class);
        return tw;
    }



    @Before
    public void setUp() throws Exception {
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
        giraphConfiguration.setComputationClass(DummyComputation.class);
        conf = new ImmutableClassesGiraphConfiguration<Text, Text,
                VIntWritable>(giraphConfiguration);
    }

    @Test
    public void testValidOutput() throws IOException, InterruptedException {
        Text expected = new Text("email@email.com\tanother@email.com\temail@email.com");
        TestWriter(expected);
    }

    @Test
    public void testNewDelimitor() throws IOException, InterruptedException {
        conf.set(LINE_TOKENIZE_VALUE, "\n");
        Text expected = new Text("email@email.com\nanother@email.com\nemail@email.com");
        TestWriter(expected);
    }

    private void TestWriter(Text expected) throws IOException, InterruptedException {
        TaskAttemptContext tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);

        DummyVertex vertex = mock(DummyVertex.class);
        when(vertex.getId()).thenReturn(new Text("email@email.com"));
        when(vertex.getValue()).thenReturn(new Text("email@email.com"));
        DummyEdge edge = mock(DummyEdge.class);
        when(edge.getTargetVertexId()).thenReturn(new Text("another@email.com"));
        // Create empty iterator == no edges
        final DummyRecordWriter tw = mock(DummyRecordWriter.class);
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
