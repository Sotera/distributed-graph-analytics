package com.soteradefense.dga.io.formats;

import com.soteradefense.dga.DGALoggingUtil;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.utils.IntPair;

import java.io.IOException;

/**
 * Read a simple undirected Edge List in csv format.  "VertexId,VertexId"  no edge values.
 */
public class DirectedIntCsvEdgeInputFormat extends TextEdgeInputFormat<IntWritable,NullWritable>{

    public EdgeReader<IntWritable, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new  DirectedIntCsvEdgeReader();
    }

    public class DirectedIntCsvEdgeReader extends  TextEdgeReaderFromEachLineProcessed<IntPair> {
        @Override
        protected IntPair preprocessLine(Text line) throws IOException {
            String[] tokens = line.toString().split(",");
            return new IntPair(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[1]));
        }

        @Override
        protected IntWritable getSourceVertexId(IntPair endpoints)
                throws IOException {
            return new IntWritable(endpoints.getFirst());
        }

        @Override
        protected IntWritable getTargetVertexId(IntPair endpoints)
                throws IOException {
            return new IntWritable(endpoints.getSecond());
        }

        @Override
        protected NullWritable getValue(IntPair endpoints) throws IOException {
            return NullWritable.get();
        }


    }

}
